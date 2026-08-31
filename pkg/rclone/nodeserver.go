/*
Copyright 2025 Veloxpack.io

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rclone

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/log"
	"github.com/rclone/rclone/fs/rc" //nolint:misspell // Don't include misspell when running golangci-lint - unknwon is the package author's username
	"github.com/rclone/rclone/lib/atexit"
	"github.com/rclone/rclone/vfs/vfscommon"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

const (
	paramCacheDir              = "cache_dir"
	paramTmpDir                = "temp_dir"
	paramLogLevel              = "log_level"
	paramMountType             = "mount_type"
	paramBackendType           = "remoteType"
	paramBackendTypeKey        = "type"
	paramRemotePrefix          = "remotePrefix"
	paramDisabledFeatures      = "disable"
	paramVolumeMountGroup      = "gid"
	paramVolumeMountUser       = "uid"
	paramVolumeMountAllowOther = "allow_other"
)

const (
	volumeLockMaxRetries = 5
	volumeLockRetryDelay = 200 * time.Millisecond

	// defaultMountTimeout bounds how long NodePublishVolume waits for a mount to
	// complete before returning and reaping the mount in the background. It is kept
	// below kubelet's ~120s NodePublishVolume timeout so the driver returns a clean,
	// retriable error rather than being torn out of the RPC mid-mount.
	defaultMountTimeout = 90 * time.Second

	// gracefulUnmountTimeout bounds how long NodeUnpublishVolume waits for a graceful
	// mountPoint.Unmount() before falling through to a forced cleanup.
	gracefulUnmountTimeout = 30 * time.Second
)

// reservedParams contains parameter names that should not be passed to rclone backend
var reservedParams = map[string]bool{
	paramRemote:       true,
	paramRemotePath:   true,
	paramConfigData:   true,
	paramBackendType:  true,
	paramRemotePrefix: true,
}

// mountContext stores context information for each mount with direct rclone objects
type mountContext struct {
	context.Context
	mountPoint *mountlib.MountPoint // Direct access to rclone mount point
	remoteName string               // Created remote name (for backwards compatibility)
	remotes    []string             // Remotes loaded for nested remotes
	cancel     context.CancelFunc   // Context cancellation for VFS goroutines
	ctx        context.Context      // Context for mount goroutines
}

// mountFilesystemFn mounts an rclone filesystem at targetPath (test seam).
type mountFilesystemFn func(
	string, string, []string, map[string]string,
) (*mountlib.MountPoint, context.Context, context.CancelFunc, error)

// mountResult carries the outcome of an asynchronous createAndMountFilesystem call
// back to the NodePublishVolume handler (or the orphan reaper).
type mountResult struct {
	mountPoint *mountlib.MountPoint
	mountCtx   context.Context
	cancel     context.CancelFunc
	err        error
}

// NodeServer implements the CSI Node service
type NodeServer struct {
	Driver            *Driver
	mounter           mount.Interface
	mountContext      map[string]*mountContext
	mountFilesystem   mountFilesystemFn
	stagedVolumes     map[string]*stagedVolume
	mountStateManager *MountStateManager
	mu                sync.RWMutex
	stagedVolumesMu   sync.RWMutex
	configMu          sync.Mutex // Protects concurrent config operations
	csi.UnimplementedNodeServer
}

// mountTimeout returns the configured mount timeout, falling back to defaultMountTimeout.
func (ns *NodeServer) mountTimeout() time.Duration {
	if ns.Driver != nil && ns.Driver.mountTimeout > 0 {
		return ns.Driver.mountTimeout
	}
	return defaultMountTimeout
}

// getMountContext retrieves mount context for a given target path
func (ns *NodeServer) getMountContext(targetPath string) *mountContext {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	if mc, ok := ns.mountContext[targetPath]; ok {
		return mc
	}
	return nil
}

// setMountContext stores mount context for a given target path
func (ns *NodeServer) setMountContext(targetPath string, mc *mountContext) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.mountContext == nil {
		ns.mountContext = make(map[string]*mountContext)
	}
	ns.mountContext[targetPath] = mc
}

// deleteMountContext removes mount context for a given target path
func (ns *NodeServer) deleteMountContext(targetPath string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	delete(ns.mountContext, targetPath)
}

// validatePublishVolumeRequest validates the NodePublishVolumeRequest
func validatePublishVolumeRequest(req *csi.NodePublishVolumeRequest) error {
	if len(req.GetVolumeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return status.Error(codes.InvalidArgument, "Target path not provided")
	}
	if req.GetVolumeCapability() == nil {
		return status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	return nil
}

// validateUnpublishVolumeRequest validates the NodeUnpublishVolumeRequest
func validateUnpublishVolumeRequest(req *csi.NodeUnpublishVolumeRequest) error {
	if len(req.GetVolumeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	return nil
}

// publishVolumeParams holds parameters for volume publishing
type publishVolumeParams struct {
	remoteName   string
	remotePath   string
	configData   string
	remoteType   string
	remotePrefix string // optional override for per-volume section prefix
	params       map[string]string
}

// setRcloneConfigFlags sets global rclone configuration flags
func setRcloneConfigFlags(ctx context.Context, ci *fs.ConfigInfo, params map[string]string) error {
	// Set cache directory if provided
	if cacheDir, ok := params[paramCacheDir]; ok {
		if err := config.SetCacheDir(cacheDir); err != nil {
			return status.Errorf(codes.Internal, "failed to set cache directory: %v", err)
		}
		klog.V(4).Infof("Set rclone cache directory to: %s", cacheDir)
	}

	// Set tmp directory if provided
	if tempDir, ok := params[paramTmpDir]; ok {
		if err := config.SetTempDir(tempDir); err != nil {
			return status.Errorf(codes.Internal, "failed to set temp directory: %v", err)
		}
		klog.V(4).Infof("Set rclone temp directory to: %s", tempDir)
	}

	// Set log level if provided
	if logLevel, ok := params[paramLogLevel]; ok {
		log.Handler.SetLevel(fs.LogLevelToSlog(fs.LogLevelDebug))
		klog.V(4).Infof("Set rclone log level to: %s", logLevel)
	}

	// Get Rclone config
	configMap := configmap.Simple{}

	// Set disabled features if provided
	if disableFeatures, ok := params[paramDisabledFeatures]; ok {
		ci.DisableFeatures = strings.Split(disableFeatures, ",")
	}

	// Set all golbal
	for key, value := range params {
		if opt := fs.ConfigOptionsInfo.Get(key); opt != nil {
			configMap.Set(key, value)
		}
	}

	// Apply the changes to the global config
	if err := configstruct.Set(configMap, ci); err != nil {
		return fmt.Errorf("failed to update global config: %v", err)
	}

	// CRITICAL: Call Reload to make changes take effect
	if err := ci.Reload(ctx); err != nil {
		return fmt.Errorf("failed to reload config changes: %v", err)
	}

	return nil
}

// mergeVolumeParameters merges driver params, secrets, and volume context
func (ns *NodeServer) mergeVolumeParameters(req *csi.NodePublishVolumeRequest) map[string]string {
	params := make(map[string]string)

	// TODO: Implement automatic cache directory generation for performance optimization
	//
	// Option 1: Shared cache per PVC (recommended for performance)
	// - Pods using same PVC share cache (warm cache on pod restart/replacement)
	// - Different PVCs remain isolated
	// - Cache lifecycle tied to PVC
	//
	// Implementation:
	//   volumeID := req.GetVolumeId()
	//   params[paramCacheDir] = filepath.Join("/var/lib/rclone-cache", volumeID)
	//   params[paramTmpDir] = filepath.Join("/tmp/rclone-temp", volumeID)
	//   klog.V(4).Infof("Using shared cache for volume %s: %s", volumeID, params[paramCacheDir])
	//
	// Option 2: Shared cache per remote location (maximum sharing)
	//   remoteName := req.GetVolumeContext()[paramRemote]
	//   remotePath := req.GetVolumeContext()[paramRemotePath]
	//   cacheKey := fmt.Sprintf("%s-%s", remoteName, remotePath)
	//   cacheHash := fmt.Sprintf("%x", sha256.Sum256([]byte(cacheKey)))[:16]
	//   params[paramCacheDir] = filepath.Join("/var/lib/rclone-cache", cacheHash)
	//
	// Option 3: Per-pod cache (maximum isolation, no sharing)
	//   uniqueID := fmt.Sprintf("%s-%d", req.GetVolumeId(), time.Now().UnixNano())
	//   params[paramCacheDir] = filepath.Join("/tmp/rclone-cache", uniqueID)
	//
	// Currently: Users must specify cache_dir and temp_dir in volume attributes/secrets
	// If not specified, rclone uses its default locations (may cause conflicts)

	// First, load values from secrets (defaults)
	secrets := req.GetSecrets()
	if secrets != nil {
		for k, v := range secrets {
			params[k] = v
		}
		klog.V(4).Infof("Loaded %d parameters from secrets", len(secrets))
	}

	// Then, merge with volumeContext (overrides secrets)
	volumeContext := req.GetVolumeContext()
	for k, v := range volumeContext {
		params[k] = v
	}

	return params
}

// extractPublishParams extracts and validates required parameters
func extractPublishParams(params map[string]string) (*publishVolumeParams, error) {
	pvp := &publishVolumeParams{
		remoteName:   params[paramRemote],
		remotePath:   params[paramRemotePath],
		configData:   params[paramConfigData],
		remoteType:   params[paramBackendType],
		remotePrefix: params[paramRemotePrefix],
		params:       make(map[string]string),
	}

	if pvp.remoteName == "" {
		return nil, status.Error(codes.InvalidArgument, "remote is required (provide via volumeAttributes or secrets)")
	}

	if pvp.configData == "" && pvp.remoteType == "" {
		return nil, status.Error(codes.InvalidArgument, "either configData or remoteType must be provided")
	}

	// Copy all params except reserved ones
	for k, v := range params {
		if !reservedParams[k] {
			rcloneKey := normalizeRcloneFlag(k)
			if rcloneKey == "" {
				return nil, status.Errorf(codes.InvalidArgument, "invalid parameter name: %s", k)
			}

			pvp.params[rcloneKey] = v
		}
	}

	return pvp, nil
}

// prepareTargetDirectory ensures the target directory exists and handles stale mounts.
// A healthy accessible mount returns errMountAlreadyHealthy so callers can decide whether
// to treat the publish as idempotent (when mountContext exists) or remount.
func (ns *NodeServer) prepareTargetDirectory(targetPath string, volumeID string) error {
	notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		switch {
		case os.IsNotExist(err):
			if err := os.MkdirAll(targetPath, 0755); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		case mount.IsCorruptedMnt(err):
			// A stale FUSE mount is left behind after a driver/node-plugin restart
			// ("transport endpoint is not connected"). Clean it up so we can remount.
			klog.Warningf("Target path %s is a corrupted mount (err: %v), cleaning up before remount", targetPath, err)
			if cerr := ns.forceCleanupMount(targetPath); cerr != nil {
				klog.Errorf("Failed to unmount corrupted mount point %s: %v", targetPath, cerr)
				return status.Errorf(codes.Internal, "corrupted mount could not be cleaned up: %v", cerr)
			}
			if err := os.MkdirAll(targetPath, 0755); err != nil {
				return status.Errorf(codes.Internal, "failed to recreate target directory after cleanup: %v", err)
			}
			klog.V(2).Infof("Successfully unmounted corrupted mount point %s, will remount", targetPath)
			notMnt = true
		default:
			return status.Error(codes.Internal, err.Error())
		}
	}

	if !notMnt {
		klog.V(2).Infof("Target path %s is already mounted", targetPath)
		_, readErr := os.ReadDir(targetPath)
		if readErr == nil {
			klog.V(4).Infof("Volume %s already mounted to %s and accessible", volumeID, targetPath)
			return errMountAlreadyHealthy
		}

		klog.Warningf(
			"Mount point %s appears mounted but is not accessible (err: %v), attempting recovery",
			targetPath, readErr,
		)
		if err := ns.forceCleanupMount(targetPath); err != nil {
			klog.Errorf("Failed to unmount corrupted mount point %s: %v", targetPath, err)
			return status.Errorf(codes.Internal, "corrupted mount could not be cleaned up: %v", err)
		}
		if err := os.MkdirAll(targetPath, 0755); err != nil {
			return status.Errorf(codes.Internal, "failed to recreate target directory after cleanup: %v", err)
		}
		klog.V(2).Infof("Successfully unmounted corrupted mount point %s, will remount", targetPath)
	}

	if err := os.Chmod(targetPath, 0755); err != nil {
		klog.Warningf("Failed to set permissions on target path %s: %v", targetPath, err)
	}

	return nil
}

// forceCleanupMount unmounts a mount point using force when available.
func (ns *NodeServer) forceCleanupMount(targetPath string) error {
	extensiveMountPointCheck := true
	forceUnmounter, ok := ns.mounter.(mount.MounterForceUnmounter)
	if ok {
		return mount.CleanupMountWithForce(targetPath, forceUnmounter, extensiveMountPointCheck, gracefulUnmountTimeout)
	}
	return mount.CleanupMountPoint(targetPath, ns.mounter, extensiveMountPointCheck)
}

func (ns *NodeServer) publishVolumeLockKey(volumeID, targetPath string) string {
	if ns.Driver != nil && ns.Driver.staging {
		return fmt.Sprintf("publish-%s-%s", volumeID, targetPath)
	}
	return fmt.Sprintf("%s-%s", volumeID, targetPath)
}

// acquireVolumeLock retries TryAcquire before returning Aborted for overlapping kubelet calls.
func (ns *NodeServer) acquireVolumeLock(lockKey, volumeID string) (func(), error) {
	for attempt := 0; attempt < volumeLockMaxRetries; attempt++ {
		if ns.Driver.volumeLocks.TryAcquire(lockKey) {
			return func() { ns.Driver.volumeLocks.Release(lockKey) }, nil
		}
		if attempt < volumeLockMaxRetries-1 {
			time.Sleep(volumeLockRetryDelay)
		}
	}
	return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
}

// generateConfigData generates rclone config from parameters if needed
func generateConfigData(pvp *publishVolumeParams) error {
	if pvp.configData == "" && pvp.remoteType != "" {
		klog.V(2).Infof("Generating dynmaic rcone config for remote type: %s", pvp.remoteType)

		// Extract remote params
		remoteParams := extractRemoteTypeParams(pvp.params, pvp.remoteType)

		if len(remoteParams) > 0 {
			pvp.configData = generateRecloneConfigFromParams(remoteParams, pvp.remoteType, pvp.remoteName)
			klog.V(4).Infof("Generated configData: %d bytes", len(pvp.configData))
		}
	}

	if pvp.configData == "" {
		return status.Error(codes.InvalidArgument, "failed to parse configData")
	}

	return nil
}

// prepareIsolatedConfig rewrites pvp.configData section names and pvp.remoteName
// so concurrent mounts with the same logical remote do not collide in LoadedData.
func (ns *NodeServer) prepareIsolatedConfig(volumeID string, pvp *publishVolumeParams) error {
	prefix := remotePrefixForVolume(volumeID, pvp.remotePrefix)
	newConfig, effectiveRemote, err := isolateConfigRemotes(pvp.configData, pvp.remoteName, prefix)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to isolate config remotes: %v", err)
	}
	klog.V(4).Infof("Isolated config for volume %s: remote %s → %s (prefix %s)",
		volumeID, pvp.remoteName, effectiveRemote, prefix)
	pvp.configData = newConfig
	pvp.remoteName = effectiveRemote
	return nil
}

// mountParamsForState returns MountParams for persistence, including remotePrefix
// when set so RemountState can re-apply the same isolation override.
func mountParamsForState(pvp *publishVolumeParams) map[string]string {
	if pvp.remotePrefix == "" {
		return pvp.params
	}
	out := maps.Clone(pvp.params)
	if out == nil {
		out = make(map[string]string)
	}
	out[paramRemotePrefix] = pvp.remotePrefix
	return out
}

// loadRcloneConfig loads config into rclone's in-memory storage
func (ns *NodeServer) loadRcloneConfig(ctx context.Context, pvp *publishVolumeParams) ([]string, error) {
	if pvp.configData == "" {
		return nil, nil
	}

	// Parse ALL remotes from configData to support nested remotes
	allRemotes, err := parseAllConfigRemotes(pvp.configData)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse configData: %v", err)
	}

	klog.V(4).Infof("Parsed %d remotes from configData", len(allRemotes))

	// Pre-allocate slice with known capacity
	remotes := make([]string, 0, len(allRemotes))

	updateRemoteOpts := config.UpdateRemoteOpt{
		NonInteractive: true,
		NoObscure:      false,
	}

	// Load all remotes into rclone's in-memory config storage
	ns.configMu.Lock()
	defer ns.configMu.Unlock()

	for remoteName, remoteData := range allRemotes {
		for key, value := range remoteData {
			// Set remote config
			config.LoadedData().SetValue(remoteName, key, value)

			// Get params for a given remote type
			if key == paramBackendTypeKey && len(pvp.params) > 0 {
				remoteParams := extractRemoteTypeParams(pvp.params, value)

				if len(remoteParams) > 0 {
					// Set the remaining values (params)
					if _, err := config.UpdateRemote(ctx, remoteName, remoteParams, updateRemoteOpts); err != nil {
						return nil, status.Errorf(codes.Internal, "failed to update remote: %v", err)
					}
				}
			}
		}
		remotes = append(remotes, remoteName)
		klog.V(4).Infof("Loaded config remote: %s with %d keys", remoteName, len(remoteData))
	}

	return remotes, nil
}

// buildFsPath constructs the filesystem path for rclone
func buildFsPath(remoteName, remotePath string) string {
	if remotePath != "" {
		return fmt.Sprintf("%s:%s", remoteName, remotePath)
	}
	return fmt.Sprintf("%s:", remoteName)
}

// cleanupConfigRemotes removes loaded remotes from rclone
func (ns *NodeServer) cleanupConfigRemotes(remotes []string) {
	if len(remotes) == 0 {
		return
	}

	ns.configMu.Lock()
	defer ns.configMu.Unlock()

	for _, remoteName := range remotes {
		config.LoadedData().DeleteSection(remoteName)
	}
	klog.V(4).Infof("Cleaned up %d remotes", len(remotes))
}

// createAndMountFilesystem initializes and mounts the rclone filesystem
func (ns *NodeServer) createAndMountFilesystem(
	fsPath, targetPath string,
	mountOptions []string,
	params map[string]string,
) (*mountlib.MountPoint, context.Context, context.CancelFunc, error) {
	// Per-mount context for config isolation and driver-owned cleanup.
	// NewFs uses WithoutCancel below so OAuth/token refresh is not tied to
	// this cancel (unpublish must not kill shared or reused Fs contexts).
	// See https://github.com/veloxpack/csi-driver-rclone/issues/54
	mountCtx, cancel := context.WithCancel(context.TODO())

	// Create per-mount context with isolated config
	mountCtx, ci := fs.AddConfig(mountCtx)

	// TODO: REVISIT - Per-mount accounting.Start(ctx) - Is this needed or is global accounting sufficient?
	// Start accounting (bandwidth limiting, stats, TPS limiting)
	// accounting.Start(mountCtx)

	// Extract volume mount options
	volumeMountOpts, err := extractVolumeMountOptions(mountOptions)
	if err != nil {
		cancel()
		return nil, nil, nil, status.Errorf(codes.Internal, "failed to parse volume mount options: %v", err)
	}

	// Merge both params and mount options
	opts := mergeCopy(params, volumeMountOpts)

	// Set rclone configuration flags
	if err := setRcloneConfigFlags(mountCtx, ci, opts); err != nil {
		cancel()
		return nil, nil, nil, err
	}

	// Config values come from mountCtx; lifetime of Fs/OAuth must outlive cancel.
	rcloneFs, err := fs.NewFs(context.WithoutCancel(mountCtx), fsPath)
	if err != nil {
		cancel()
		return nil, nil, nil, status.Errorf(codes.Internal, "failed to initialize filesystem: %v", err)
	}

	// Extract Rclone mount options
	mountOpts, err := extractMountOptions(opts)
	if err != nil {
		cancel()
		return nil, nil, nil, status.Errorf(codes.Internal, "failed to parse mount options: %v", err)
	}

	// Extract Rclone VFS options
	vfsOpts, err := extractVFSOptions(opts)
	if err != nil {
		cancel()
		return nil, nil, nil, status.Errorf(codes.Internal, "failed to parse VFS options: %v", err)
	}

	// Set device name if not already set
	if mountOpts.DeviceName == "" {
		mountOpts.DeviceName = fsPath
	}

	// Get mount function with enhanced resolution
	mountType, mountFn, err := resolveMountMethod(opts)
	if err != nil {
		cancel()
		return nil, nil, nil, status.Errorf(codes.InvalidArgument, "mount method resolution failed: %v", err)
	}

	klog.V(4).Infof("Using mount method: %s", mountType)

	// Create mount point
	mountPoint := mountlib.NewMountPoint(mountFn, targetPath, rcloneFs, mountOpts, vfsOpts)

	// Mount the filesystem
	mountDaemon, err := mountPoint.Mount()
	if err != nil {
		cancel()
		return nil, nil, nil, status.Errorf(codes.Internal, "failed to mount: %v", err)
	}

	// Handle rclone daemon if needed
	if err := handleRcloneDaemon(mountPoint, mountDaemon, mountOpts, cancel); err != nil {
		cancel()
		return nil, nil, nil, err
	}

	return mountPoint, mountCtx, cancel, nil
}

// handleRcloneDaemon manages rclone daemon lifecycle for mount operations.
// It handles daemon process management, timeout waiting, and proper cleanup
// when daemon mode is enabled in mount options.
func handleRcloneDaemon(
	mountPoint *mountlib.MountPoint,
	mountDaemon *os.Process,
	mountOpts *mountlib.Options,
	cancel context.CancelFunc,
) error {
	if mountOpts.Daemon {
		config.PassConfigKeyForDaemonization = true
	}

	if mountOpts.DaemonWait <= 0 {
		// No daemon wait configured, nothing to do
		return nil
	}

	// Wait for mountDaemon, if any...
	killDaemon := sync.OnceFunc(func() {
		if err := mountDaemon.Signal(os.Interrupt); err != nil {
			klog.Errorf("Failed to terminate daemon pid %d: %v", mountDaemon.Pid, err)
			return
		}
		klog.V(2).Infof("Terminating daemon pid %d", mountDaemon.Pid)
	})

	handle := atexit.Register(func() {
		klog.V(2).Info("Got interrupt")
		killDaemon()
	})

	defer atexit.Unregister(handle)

	if err := mountlib.WaitMountReady(
		mountPoint.MountPoint, time.Duration(mountOpts.DaemonWait), mountDaemon,
	); err != nil {
		klog.V(2).Info("Daemon timed out")
		killDaemon()
		cancel()
		return status.Errorf(codes.Internal, "failed to wait for mount: %v", err)
	}

	return nil
}

// waitForVFSCacheSync waits for VFS cache uploads to complete before unmount
func waitForVFSCacheSync(mc *mountContext) {
	if mc == nil || mc.mountPoint == nil {
		return
	}

	// Get VFS stats to check if cache is enabled
	stats := mc.mountPoint.VFS.Stats()

	// Check if diskCache is present (only when cache mode > off)
	_, hasDiskCache := stats["diskCache"].(rc.Params)
	if !hasDiskCache {
		klog.V(4).Infof("VFS cache mode is off, no cache sync needed")
		return
	}

	klog.V(2).Infof("Waiting for VFS cache sync (remote: %s)", mc.remoteName)

	timeout := time.Now().Add(2 * time.Minute)
	retryCount := 0
	maxRetries := 5

	for time.Now().Before(timeout) && retryCount < maxRetries {
		allClear := true

		stats := mc.mountPoint.VFS.Stats()
		if diskCache, ok := stats["diskCache"].(rc.Params); ok {
			uploadsInProgress, _ := diskCache["uploadsInProgress"].(int)
			uploadsQueued, _ := diskCache["uploadsQueued"].(int)

			if uploadsInProgress > 0 || uploadsQueued > 0 {
				klog.V(4).Infof(
					"Waiting for VFS cache uploads (in progress: %d, queued: %d, retry: %d/%d)",
					uploadsInProgress, uploadsQueued, retryCount+1, maxRetries,
				)
				allClear = false
			}
		} else {
			klog.Warningf("Failed to get VFS cache stats, retry %d/%d", retryCount+1, maxRetries)
			allClear = false
		}

		if allClear {
			break
		}

		retryCount++
		// Exponential backoff for better performance
		sleepDuration := time.Duration(retryCount) * 2 * time.Second
		if sleepDuration > 10*time.Second {
			sleepDuration = 10 * time.Second
		}
		time.Sleep(sleepDuration)
	}

	if retryCount >= maxRetries {
		klog.Warningf("VFS cache sync timeout after %d retries, proceeding with unmount", maxRetries)
	}

	klog.V(2).Infof("Cache sync complete, proceeding with unmount")
}

// extractVFSOptions extracts and configures VFS (Virtual File System) options from parameters.
// It loads the default VFS options from rclone's configuration system and then applies
// any overrides provided in the params map. This allows the CSI driver to customize
// VFS behavior such as caching, read-ahead, and file permissions based on volume
// configuration parameters.
func extractVFSOptions(params map[string]string) (*vfscommon.Options, error) {
	vfsOpts := new(vfscommon.Options)

	// Load VFS options from parsed flags
	configMap := fs.ConfigMap("", vfscommon.OptionsInfo, "", nil)
	if err := configstruct.Set(configMap, vfsOpts); err != nil {
		return nil, fmt.Errorf("failed to load VFS options: %v", err)
	}

	// Create a mutable config map and update it
	mutableMap := configmap.Simple{}

	// Copy existing values from the read-only config map
	for _, opt := range vfscommon.OptionsInfo {
		// Set defaults
		if value, ok := configMap.Get(opt.Name); ok {
			mutableMap.Set(opt.Name, value)
		}

		// Override with vfs options in the params
		if value, ok := params[opt.Name]; ok {
			mutableMap.Set(opt.Name, value)
		}
	}

	// update the mutable config
	if err := configstruct.Set(mutableMap, vfsOpts); err != nil {
		return nil, fmt.Errorf("failed to update VFS options: %v", err)
	}

	return vfsOpts, nil
}

// resolveMountMethod resolves the mount method for the current platform.
// It supports user-specified mount methods via the "mount_type" parameter and falls back
// to rclone's default mount method resolution.
//
// If user specifies a mount type, it tries that first. If not available, it returns an error.
// If not specified, it falls back to rclone's default mount method resolution.
func resolveMountMethod(params map[string]string) (string, mountlib.MountFn, error) {
	// Check if user specified a mount type
	if mountType, ok := params[paramMountType]; ok {
		klog.V(4).Infof("Specified mount type: %s", mountType)
		// Try the specified mount type first
		if resolvedType, mountFn := mountlib.ResolveMountMethod(mountType); mountFn != nil {
			return resolvedType, mountFn, nil
		}
		return "", nil, fmt.Errorf("specified mount type '%s' not available", mountType)
	}

	// Fallback to rclone's default resolution
	mountType, mountFn := mountlib.ResolveMountMethod("")
	if mountFn != nil {
		klog.V(4).Infof("Using rclone default mount method: %s", mountType)
		return mountType, mountFn, nil
	}

	return "", nil, fmt.Errorf("no mount methods available")
}

// extractMountOptions extracts and configures mount options from parameters.
// It loads the default mount options from rclone's configuration system and then applies
// any overrides provided in the params map. This allows the CSI driver to customize
// mount behavior such as FUSE options, permissions, and performance settings based on
// volume configuration parameters.
func extractMountOptions(params map[string]string) (*mountlib.Options, error) {
	mountOpts := new(mountlib.Options)

	// Load mount options from parsed flags
	configMap := fs.ConfigMap("", mountlib.OptionsInfo, "", nil)
	if err := configstruct.Set(configMap, mountOpts); err != nil {
		return nil, fmt.Errorf("failed to load mount options: %v", err)
	}

	// Create a mutable config map and update it
	mutableMap := configmap.Simple{}

	// Copy existing values from the read-only config map
	for _, opt := range mountlib.OptionsInfo {
		// Set defaults
		if value, ok := configMap.Get(opt.Name); ok {
			mutableMap.Set(opt.Name, value)
		}

		// Override with mount options in the params
		if value, ok := params[opt.Name]; ok {
			mutableMap.Set(opt.Name, value)
		}
	}

	// update the mutable config
	if err := configstruct.Set(mutableMap, mountOpts); err != nil {
		return nil, fmt.Errorf("failed to update mount options: %v", err)
	}

	return mountOpts, nil
}

// extractVolumeMountOptions parses CSI mount options into a key-value map.
// It handles both key=value format options and boolean flags (without values).
// Boolean flags are automatically set to "true" when no value is provided.
//
// This function is used to convert mount options from the CSI NodePublishVolume
// request into a format that can be used with rclone's configuration system.
//
// Supported formats:
//   - "key=value" -> map["key"] = "value"
//   - "boolean_flag" -> map["boolean_flag"] = "true"
//
// Example:
//
//	Input:  ["ro", "noatime", "uid=1000", "gid=1000"]
//	Output: map[string]string{
//	          "ro": "true",
//	          "noatime": "true",
//	          "uid": "1000",
//	          "gid": "1000"
//	        }
func extractVolumeMountOptions(mountOptions []string) (map[string]string, error) {
	volumeMountOptions := make(map[string]string)

	for _, option := range mountOptions {
		if strings.Contains(option, "=") {
			parts := strings.SplitN(option, "=", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid mount option format: %s", option)
			}

			rcloneKey := normalizeRcloneFlag(parts[0])
			volumeMountOptions[rcloneKey] = parts[1]
		} else {
			rcloneKey := normalizeRcloneFlag(option)
			// Default a boolean value
			volumeMountOptions[rcloneKey] = trueValue
		}
	}

	return volumeMountOptions, nil
}

// unmountVolume unmounts the volume and performs cleanup
func (ns *NodeServer) unmountVolume(mc *mountContext, targetPath string) error {
	if mc != nil && mc.mountPoint != nil {
		// Wait for cache sync
		waitForVFSCacheSync(mc)

		// Unmount using mountPoint's built-in unmount. This call is unbounded and can
		// wedge on in-flight S3 uploads, so bound it with a timeout and fall through to
		// the forced CleanupMountWithForce below regardless (issue #86).
		unmountDone := make(chan error, 1)
		go func() { unmountDone <- mc.mountPoint.Unmount() }()
		select {
		case err := <-unmountDone:
			if err != nil {
				klog.Errorf("Failed to unmount via mountPoint: %v, will try standard unmount", err)
			} else {
				klog.V(4).Infof("Successfully unmounted via mountPoint.Unmount()")
			}
		case <-time.After(gracefulUnmountTimeout):
			klog.Warningf("Graceful unmount of %s timed out after %s, will force unmount", targetPath, gracefulUnmountTimeout)
		}

		// Cancel context to stop VFS goroutines
		if mc.cancel != nil {
			mc.cancel()
		}

		// Clean up loaded remotes
		if len(mc.remotes) > 0 {
			ns.configMu.Lock()
			for _, remoteName := range mc.remotes {
				config.LoadedData().DeleteSection(remoteName)
			}
			ns.configMu.Unlock()
			klog.V(4).Infof("Deleted %d remotes from config", len(mc.remotes))
		}
	}

	// Use k8s mounter as fallback for cleanup
	klog.V(2).Infof("Performing final unmount cleanup for %s", targetPath)
	var err error
	extensiveMountPointCheck := true
	forceUnmounter, ok := ns.mounter.(mount.MounterForceUnmounter)
	if ok {
		klog.V(4).Infof("Using force unmount with %s timeout", gracefulUnmountTimeout)
		err = mount.CleanupMountWithForce(targetPath, forceUnmounter, extensiveMountPointCheck, gracefulUnmountTimeout)
	} else {
		klog.V(4).Infof("Using standard cleanup")
		err = mount.CleanupMountPoint(targetPath, ns.mounter, extensiveMountPointCheck)
	}

	return err
}

// NodePublishVolume publishes the volume directly or from a staged mount.
//
//nolint:lll
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if ns.Driver == nil || !ns.Driver.staging {
		return ns.nodePublishVolumeDirect(ctx, req)
	}
	return ns.nodePublishVolumeStaged(ctx, req)
}

// nodePublishVolumeDirect mounts the rclone volume using direct rclone library integration.
//
//nolint:lll
func (ns *NodeServer) nodePublishVolumeDirect(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// Validate request
	if err := validatePublishVolumeRequest(req); err != nil {
		return nil, err
	}

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	mountCap := req.GetVolumeCapability().GetMount()

	// Acquire lock for this volume operation
	lockKey := ns.publishVolumeLockKey(volumeID, targetPath)
	_, err := ns.acquireVolumeLock(lockKey, volumeID)
	if err != nil {
		return nil, err
	}

	// mountSuccess reports whether the mount completed and its context was stored.
	// handOff reports that ownership of the in-flight mount, its loaded remotes, and
	// the volume lock has been transferred to reapOrphanMount (on timeout/cancel). When
	// handed off, neither the lock nor the remotes are released here — the reaper does it.
	var mountSuccess, handOff bool
	defer func() {
		if !handOff {
			ns.Driver.volumeLocks.Release(lockKey)
		}
	}()

	// Get mount options from VolumeCapability (CSI standard)
	readOnly := req.GetReadonly()
	var mountOptions []string

	if mountCap != nil {
		mountOptions = mountCap.GetMountFlags()
		if readOnly {
			mountOptions = append(mountOptions, "read-only")
		}
	}

	// Merge parameters from secrets and volume context
	params := ns.mergeVolumeParameters(req)

	// Extract volume mount group from fsGroup in CSI request.
	if mountCap != nil {
		volumeMountGroup := mountCap.GetVolumeMountGroup()
		if volumeMountGroup != "" {
			params[paramVolumeMountGroup] = volumeMountGroup
			params[paramVolumeMountUser] = volumeMountGroup
			params[paramVolumeMountAllowOther] = "true"
			klog.V(2).Infof("Volume mount group: %s, user: %s", volumeMountGroup, params[paramVolumeMountUser])
		}
	}

	// Extract and validate required parameters
	pvp, err := extractPublishParams(params)
	if err != nil {
		return nil, err
	}

	// Prepare target directory and check if already mounted. Healthy mounts without
	// driver mountContext are cleaned up and remounted (recovery after plugin restart).
	if err := ns.prepareTargetDirectory(targetPath, volumeID); err != nil {
		if errors.Is(err, errMountAlreadyHealthy) {
			if ns.getMountContext(targetPath) != nil {
				klog.V(2).Infof("Volume %s already published at %s", volumeID, targetPath)
				return &csi.NodePublishVolumeResponse{}, nil
			}
			klog.V(2).Infof("Volume %s has healthy mount at %s but no driver context, remounting", volumeID, targetPath)
			if err := ns.forceCleanupMount(targetPath); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to cleanup stale mount before remount: %v", err)
			}
			if err := os.MkdirAll(targetPath, 0755); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to recreate target directory after cleanup: %v", err)
			}
		} else {
			return nil, err
		}
	}

	klog.V(2).Infof("NodePublishVolume: mounting %s:%s at %s", pvp.remoteName, pvp.remotePath, targetPath)

	// Generate config data if needed
	if err := generateConfigData(pvp); err != nil {
		return nil, err
	}

	// Persist logical/original config; isolate only for in-process LoadedData.
	originalConfigData := pvp.configData
	originalRemoteName := pvp.remoteName
	if err := ns.prepareIsolatedConfig(volumeID, pvp); err != nil {
		return nil, err
	}

	// Load rclone config
	remotes, err := ns.loadRcloneConfig(ctx, pvp)
	if err != nil {
		return nil, err
	}

	// Build filesystem path
	fsPath := buildFsPath(pvp.remoteName, pvp.remotePath)
	klog.V(2).Infof("Using configData with %d remotes, resolving remote: %s", len(remotes), fsPath)

	// Ensure loaded remotes are cleaned up on failure. On hand-off (timeout/cancel),
	// ownership transfers to the reaper, which frees them once the background mount ends.
	defer func() {
		if !mountSuccess && !handOff {
			ns.cleanupConfigRemotes(remotes)
		}
	}()

	// Run the mount in a background goroutine so we can bound how long we hold the
	// volume lock. fs.NewFs and mountPoint.Mount() do S3 network I/O that can block
	// indefinitely under throttling; without a bound the deferred lock release would
	// never fire and every kubelet retry would get Aborted forever (issue #86).
	// The channel is buffered (cap 1) so the goroutine's send never blocks even after
	// this handler has returned on the timeout/cancel path.
	resultCh := make(chan mountResult, 1)
	go func() {
		mp, mctx, cancel, err := ns.createAndMountFilesystem(fsPath, targetPath, mountOptions, pvp.params)
		resultCh <- mountResult{mountPoint: mp, mountCtx: mctx, cancel: cancel, err: err}
	}()

	timer := time.NewTimer(ns.mountTimeout())
	defer timer.Stop()

	select {
	case res := <-resultCh:
		if res.err != nil {
			// Pre-timeout failure: mountSuccess stays false, so the deferred
			// cleanupConfigRemotes frees the loaded remotes. createAndMountFilesystem
			// already cancelled its context on every error path.
			return nil, res.err
		}

		mountSuccess = true

		// Store mount context
		ns.setMountContext(targetPath, &mountContext{
			mountPoint: res.mountPoint,
			remoteName: pvp.remoteName,
			remotes:    remotes,
			cancel:     res.cancel,
			ctx:        res.mountCtx,
		})

		if ns.mountStateManager != nil {
			state := &MountState{
				VolumeID:     volumeID,
				TargetPath:   targetPath,
				Timestamp:    time.Now(),
				ConfigData:   originalConfigData,
				RemoteName:   originalRemoteName,
				RemotePath:   pvp.remotePath,
				RemoteType:   pvp.remoteType,
				MountParams:  mountParamsForState(pvp),
				MountOptions: mountOptions,
				ReadOnly:     readOnly,
			}
			if err := ns.mountStateManager.SaveState(res.mountCtx, state); err != nil {
				klog.Warningf("Failed to save mount state for volume %s: %v", volumeID, err)
			}
		}

		klog.V(2).Infof("Successfully mounted volume %s to %s (remote: %s)", volumeID, targetPath, pvp.remoteName)
		return &csi.NodePublishVolumeResponse{}, nil

	case <-timer.C:
		// Mount is taking too long. Hand off ownership of the in-flight mount, the
		// loaded remotes, and the lock to the reaper so we return promptly and the
		// lock is always eventually released. Return Aborted (retriable by kubelet).
		handOff = true
		klog.Warningf("NodePublishVolume for %s timed out after %s; reaping mount in background", targetPath, ns.mountTimeout())
		go ns.reapOrphanMount(resultCh, targetPath, remotes, lockKey)
		return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)

	case <-ctx.Done():
		// kubelet cancelled/deadlined the RPC before our own timeout. Same hand-off.
		handOff = true
		klog.Warningf("NodePublishVolume for %s cancelled by caller (%v); reaping mount in background", targetPath, ctx.Err())
		go ns.reapOrphanMount(resultCh, targetPath, remotes, lockKey)
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// reapOrphanMount takes ownership of an in-flight mount whose NodePublishVolume handler
// already returned (on timeout or caller cancellation). It waits for the background mount
// to finish, tears it down if it actually came up, frees the loaded remotes, and only then
// releases the volume lock — so kubelet retries are refused until the orphan is fully gone
// and can never observe the about-to-be-torn-down mount as "already mounted".
//
//nolint:lll
func (ns *NodeServer) reapOrphanMount(resultCh <-chan mountResult, targetPath string, remotes []string, lockKey string) {
	defer ns.Driver.volumeLocks.Release(lockKey)

	res := <-resultCh // buffered send guarantees this arrives

	if res.err != nil {
		// createAndMountFilesystem cancelled its context on every error path, so nothing
		// is mounted. Only the loaded remotes remain to be dropped.
		klog.Warningf("Orphan mount for %s failed after hand-off: %v", targetPath, res.err)
		ns.cleanupConfigRemotes(remotes)
		return
	}

	// The mount actually came up, but the caller was already told it failed. Tear it down
	// completely so a subsequent retry starts clean. No VFS cache sync: the caller never
	// saw this mount, there is nothing to flush, and syncing could block while we hold the lock.
	klog.Warningf("Orphan mount for %s succeeded after hand-off; unmounting to avoid leak", targetPath)
	if res.cancel != nil {
		res.cancel() // stop VFS/mount goroutines and OAuth ctx before detaching
	}
	ns.forceUnmountBounded(targetPath)
	ns.cleanupConfigRemotes(remotes)
}

// forceUnmountBounded detaches a mount at targetPath with a bounded timeout, preferring a
// force unmount when the mounter supports it. It never calls the unbounded mountPoint.Unmount().
func (ns *NodeServer) forceUnmountBounded(targetPath string) {
	extensiveMountPointCheck := true
	if forceUnmounter, ok := ns.mounter.(mount.MounterForceUnmounter); ok {
		err := mount.CleanupMountWithForce(targetPath, forceUnmounter, extensiveMountPointCheck, gracefulUnmountTimeout)
		if err != nil {
			klog.Errorf("Orphan force-unmount of %s failed: %v", targetPath, err)
		}
		return
	}
	if err := mount.CleanupMountPoint(targetPath, ns.mounter, extensiveMountPointCheck); err != nil {
		klog.Errorf("Orphan cleanup of %s failed: %v", targetPath, err)
	}
}

func (ns *NodeServer) nodePublishVolumeStaged(
	ctx context.Context, req *csi.NodePublishVolumeRequest,
) (*csi.NodePublishVolumeResponse, error) {
	if err := validatePublishVolumeRequest(req); err != nil {
		return nil, err
	}

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	lockKey := ns.publishVolumeLockKey(volumeID, targetPath)
	release, err := ns.acquireVolumeLock(lockKey, volumeID)
	if err != nil {
		return nil, err
	}
	defer release()

	stagingPath := req.GetStagingTargetPath()
	if stagingPath == "" {
		stagingPath = ns.getStagingPathForPublish(ctx, volumeID)
	}
	healthy, _ := ns.stageMountHealthy(stagingPath)
	if ns.getStagedVolume(volumeID) == nil || !healthy {
		if !healthy {
			ns.deleteStagedVolume(volumeID)
		}
		stageLockKey := fmt.Sprintf("stage-%s", volumeID)
		stageRelease, err := ns.acquireVolumeLock(stageLockKey, volumeID)
		if err != nil {
			return nil, err
		}
		if err := ns.rebuildOrRestage(ctx, req, stagingPath); err != nil {
			stageRelease()
			return nil, err
		}
		stageRelease()
	}

	if err := ns.prepareTargetDirectory(targetPath, volumeID); err != nil {
		if errors.Is(err, errMountAlreadyHealthy) {
			klog.V(2).Infof("Volume %s already published at %s", volumeID, targetPath)
			return &csi.NodePublishVolumeResponse{}, nil
		}
		return nil, err
	}

	if err := ns.bindPublish(stagingPath, targetPath, req.GetReadonly()); err != nil {
		return nil, err
	}

	klog.V(2).Infof("Successfully bind-published volume %s from %s to %s", volumeID, stagingPath, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the rclone volume using direct stats access
//
//nolint:lll
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	// Validate request
	if err := validateUnpublishVolumeRequest(req); err != nil {
		return nil, err
	}

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	// Acquire lock for this volume operation
	lockKey := ns.publishVolumeLockKey(volumeID, targetPath)
	release, err := ns.acquireVolumeLock(lockKey, volumeID)
	if err != nil {
		return nil, err
	}
	defer release()

	klog.V(2).Infof("NodeUnpublishVolume: unmounting volume %s from %s", volumeID, targetPath)

	if ns.Driver != nil && ns.Driver.staging {
		if err := ns.unbindPublish(targetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount target %q: %v", targetPath, err)
		}
		klog.V(2).Infof("Successfully unpublished bind mount for volume %s from %s", volumeID, targetPath)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// Get mount context and unmount
	mc := ns.getMountContext(targetPath)
	if err := ns.unmountVolume(mc, targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount target %q: %v", targetPath, err)
	}

	ns.deleteMountContext(targetPath)

	if ns.mountStateManager != nil {
		if err := ns.mountStateManager.DeleteState(ctx, volumeID, targetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "delete mount state for volume %s: %v", volumeID, err)
		}
	}

	klog.V(2).Infof("Successfully unmounted volume %s from %s", volumeID, targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// isMountHealthy checks if a mount is healthy and returns a detailed error message
func (ns *NodeServer) isMountHealthy(targetPath string) (bool, string) {
	healthy, msg := IsMountPathHealthy(targetPath, ns.mounter)
	if !healthy {
		return false, msg
	}

	// Check VFS stats for errors if mount context is available
	if mc := ns.getMountContext(targetPath); mc != nil {
		if mc.mountPoint != nil && mc.mountPoint.VFS != nil {
			stats := mc.mountPoint.VFS.Stats()
			if errCount, ok := stats["errors"]; ok && errCount.(int) > 0 {
				return false, fmt.Sprintf("VFS errors detected: %d", errCount.(int))
			}
		}
	}

	// If we can read the directory, consider it healthy even if we don't have mount context
	// (this handles edge cases where mount context might be missing but mount is working)
	return true, ""
}

// NodeGetInfo returns info about the node
func (ns *NodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.Driver.nodeID,
	}, nil
}

// NodeGetCapabilities returns the capabilities of the node
//
//nolint:lll
func (ns *NodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: ns.Driver.nscap,
	}, nil
}

// NodeGetVolumeStats returns volume stats and health condition
//
//nolint:lll
func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	volumePath := req.GetVolumePath()
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}

	klog.V(4).Infof("NodeGetVolumeStats called for volume path: %s", volumePath)

	// Get filesystem statistics
	var statfs unix.Statfs_t
	if err := unix.Statfs(volumePath, &statfs); err != nil {
		klog.Errorf("Failed to get filesystem stats for %s: %v", volumePath, err)
		// Return abnormal condition if we can't read stats
		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{},
			VolumeCondition: &csi.VolumeCondition{
				Abnormal: true,
				Message:  fmt.Sprintf("Failed to get filesystem statistics: %v", err),
			},
		}, nil
	}

	// Calculate volume usage in bytes
	// Note: Bsize might be different on different platforms, so we use int64 to ensure compatibility
	blockSize := int64(statfs.Bsize) //nolint:unconvert // Bsize type varies by platform (uint32 on darwin, int64 on linux)
	totalBytes := int64(statfs.Blocks) * blockSize
	availableBytes := int64(statfs.Bavail) * blockSize
	freeBytes := int64(statfs.Bfree) * blockSize
	usedBytes := totalBytes - freeBytes

	// Get inode statistics
	totalInodes := int64(statfs.Files)
	freeInodes := int64(statfs.Ffree)
	usedInodes := totalInodes - freeInodes

	// Build volume usage for bytes
	usage := []*csi.VolumeUsage{
		{
			Available: availableBytes,
			Total:     totalBytes,
			Used:      usedBytes,
			Unit:      csi.VolumeUsage_BYTES,
		},
	}

	// Add inode usage if available
	if totalInodes > 0 {
		usage = append(usage, &csi.VolumeUsage{
			Available: freeInodes,
			Total:     totalInodes,
			Used:      usedInodes,
			Unit:      csi.VolumeUsage_INODES,
		})
	}

	// Check staging health for bind-published volumes; usage still comes from the requested path.
	healthPath := volumePath
	if ns.Driver != nil && ns.Driver.staging && req.GetVolumeId() != "" {
		healthPath = ns.getStagingPathForPublish(ctx, req.GetVolumeId())
	}
	healthy, healthMessage := ns.isMountHealthy(healthPath)
	volumeCondition := &csi.VolumeCondition{
		Abnormal: !healthy,
		Message:  healthMessage,
	}

	if healthy {
		volumeCondition.Message = "Volume is healthy and accessible"
	}

	klog.V(4).Infof("Volume stats for %s: Total=%d bytes, Available=%d bytes, Used=%d bytes, Healthy=%v",
		volumePath, totalBytes, availableBytes, usedBytes, healthy)

	return &csi.NodeGetVolumeStatsResponse{
		Usage:           usage,
		VolumeCondition: volumeCondition,
	}, nil
}

// NodeExpandVolume is not implemented
//
//nolint:lll
func (ns *NodeServer) NodeExpandVolume(_ context.Context, _ *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// UnmountAll unmounts all active volume mounts gracefully.
func (ns *NodeServer) UnmountAll(ctx context.Context) error {
	ns.mu.RLock()
	mountContexts := maps.Clone(ns.mountContext)
	ns.mu.RUnlock()

	if len(mountContexts) == 0 {
		klog.Info("No active mounts to unmount")
		return nil
	}

	klog.Infof("Unmounting %d active volumes", len(mountContexts))

	var wg sync.WaitGroup
	errorChan := make(chan error, len(mountContexts))

	for targetPath, mc := range mountContexts {
		path, mctx := targetPath, mc
		wg.Go(func() {
			if err := ns.unmountVolume(mctx, path); err != nil {
				errorChan <- fmt.Errorf("failed to unmount %s: %w", path, err)
				return
			}
			ns.deleteMountContext(path)
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
		close(errorChan)
	}()

	select {
	case <-done:
		var errs []error
		for err := range errorChan {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	case <-ctx.Done():
		return fmt.Errorf("unmount timeout: %w", ctx.Err())
	}
}

// RemountState remounts a volume from a saved MountState after driver restart.
func (ns *NodeServer) RemountState(ctx context.Context, state *MountState) error {
	if err := state.Validate(); err != nil {
		return fmt.Errorf("invalid mount state: %w", err)
	}

	mountPath := state.StagingPath
	if mountPath == "" {
		mountPath = state.TargetPath
	}

	klog.V(2).Infof("Remounting volume %s to %s (remote: %s)", state.VolumeID, mountPath, state.RemoteName)

	if err := ns.prepareTargetDirectory(mountPath, state.VolumeID); err != nil {
		if !errors.Is(err, errMountAlreadyHealthy) {
			return fmt.Errorf("prepare target directory: %w", err)
		}
		if ns.Driver != nil && ns.Driver.staging && isLikelyStagingPath(mountPath) {
			if err := ns.rebuildStagedVolumeAfterRemount(state, mountPath); err != nil {
				return err
			}
			return nil
		}
		if ns.getMountContext(mountPath) != nil {
			return nil
		}
		if err := ns.forceCleanupMount(mountPath); err != nil {
			return fmt.Errorf("cleanup stale mount: %w", err)
		}
		if err := os.MkdirAll(mountPath, 0755); err != nil {
			return fmt.Errorf("recreate target directory: %w", err)
		}
	}

	notMnt, err := ns.mounter.IsLikelyNotMountPoint(mountPath)
	if err == nil && !notMnt {
		if mc := ns.getMountContext(mountPath); mc != nil {
			if err := ns.unmountVolume(mc, mountPath); err != nil {
				klog.Warningf("Failed to unmount existing mount at %s: %v", mountPath, err)
			}
			ns.deleteMountContext(mountPath)
		} else if err := ns.forceCleanupMount(mountPath); err != nil {
			klog.Warningf("Failed to force cleanup mount at %s: %v", mountPath, err)
		}
		if err := os.MkdirAll(mountPath, 0755); err != nil {
			return fmt.Errorf("recreate target directory: %w", err)
		}
	}

	mountParams := state.MountParams
	remotePrefix := ""
	if mountParams != nil {
		if v, ok := mountParams[paramRemotePrefix]; ok {
			remotePrefix = v
			mountParams = maps.Clone(mountParams)
			delete(mountParams, paramRemotePrefix)
		}
	}

	pvp := &publishVolumeParams{
		remoteName:   state.RemoteName,
		remotePath:   state.RemotePath,
		configData:   state.ConfigData,
		remoteType:   state.RemoteType,
		remotePrefix: remotePrefix,
		params:       mountParams,
	}

	if err := ns.prepareIsolatedConfig(state.VolumeID, pvp); err != nil {
		return fmt.Errorf("failed to isolate config: %w", err)
	}

	remotes, err := ns.loadRcloneConfig(ctx, pvp)
	if err != nil {
		return fmt.Errorf("failed to load rclone config: %w", err)
	}

	var mountSuccess bool
	defer func() {
		if !mountSuccess {
			ns.cleanupConfigRemotes(remotes)
		}
	}()

	fsPath := buildFsPath(pvp.remoteName, pvp.remotePath)
	mountPoint, _, cancel, err := ns.mountRcloneFilesystem(
		fsPath, mountPath, state.MountOptions, pvp.params,
	)
	if err != nil {
		return fmt.Errorf("failed to mount filesystem: %w", err)
	}

	mountSuccess = true
	mc := &mountContext{
		mountPoint: mountPoint,
		remoteName: pvp.remoteName,
		remotes:    remotes,
		cancel:     cancel,
	}
	ns.setMountContext(mountPath, mc)

	if ns.mountStateManager != nil {
		state.Timestamp = time.Now()
		if err := ns.mountStateManager.SaveState(ctx, state); err != nil {
			klog.Warningf("Failed to update mount state for volume %s: %v", state.VolumeID, err)
		}
	}

	if ns.Driver != nil && ns.Driver.staging && isLikelyStagingPath(mountPath) {
		ns.setStagedVolume(state.VolumeID, &stagedVolume{
			volumeID:    state.VolumeID,
			stagingPath: mountPath,
			mountCtx:    mc,
		})
		// Skip publish bind refresh on remount boot. WalkDir/ReadDir into a live
		// bind of this process's FUSE deadlocks (request_wait_answer vs
		// fuse_dev_do_read) before gRPC listen — especially vfs-cache-mode=writes.
		// Volume-recovery-operator restarts workloads; NodePublish creates fresh binds.
		klog.V(2).Infof(
			"Skipping publish bind refresh after remount of %s; workloads need restart for new binds",
			state.VolumeID,
		)
	}

	klog.V(2).Infof("Successfully remounted volume %s to %s", state.VolumeID, mountPath)
	return nil
}

// RemountAllStates loads saved mount states for this node and remounts them.
func (ns *NodeServer) RemountAllStates(ctx context.Context) error {
	if ns.mountStateManager == nil {
		return nil
	}

	states, err := ns.mountStateManager.LoadState(ctx)
	if err != nil {
		return fmt.Errorf("failed to load mount states: %w", err)
	}

	if len(states) == 0 {
		return nil
	}

	klog.Infof("Remounting %d persisted mount states", len(states))

	errorCount := 0
	for _, state := range states {
		if err := ns.RemountState(ctx, state); err != nil {
			klog.Errorf("Failed to remount volume %s: %v", state.VolumeID, err)
			errorCount++
		}
	}

	if errorCount > 0 {
		return fmt.Errorf("failed to remount %d of %d volumes", errorCount, len(states))
	}
	return nil
}
