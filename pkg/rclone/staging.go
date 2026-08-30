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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

var kubeletPodsDir = "/var/lib/kubelet/pods"

const csiPublishMountDir = "mount"

// stagedVolume tracks a FUSE mount at the node-global staging path for a volume.
type stagedVolume struct {
	volumeID    string
	stagingPath string
	mountCtx    *mountContext
	readOnly    bool
}

// getStagedVolume retrieves the staged volume cache entry for volumeID.
func (ns *NodeServer) getStagedVolume(volumeID string) *stagedVolume {
	ns.stagedVolumesMu.RLock()
	defer ns.stagedVolumesMu.RUnlock()
	if sv, ok := ns.stagedVolumes[volumeID]; ok {
		return sv
	}
	return nil
}

// setStagedVolume stores a staged volume cache entry for volumeID.
func (ns *NodeServer) setStagedVolume(volumeID string, sv *stagedVolume) {
	ns.stagedVolumesMu.Lock()
	defer ns.stagedVolumesMu.Unlock()
	if ns.stagedVolumes == nil {
		ns.stagedVolumes = make(map[string]*stagedVolume)
	}
	ns.stagedVolumes[volumeID] = sv
}

// deleteStagedVolume removes the staged volume cache entry for volumeID.
func (ns *NodeServer) deleteStagedVolume(volumeID string) {
	ns.stagedVolumesMu.Lock()
	defer ns.stagedVolumesMu.Unlock()
	delete(ns.stagedVolumes, volumeID)
}

func (ns *NodeServer) getDefaultStagingPath(volumeID string) string {
	driverName := DefaultDriverName
	if ns.Driver != nil && ns.Driver.name != "" {
		driverName = ns.Driver.name
	}
	return filepath.Join(
		"/var/lib/kubelet/plugins/kubernetes.io/csi",
		driverName,
		"pv",
		volumeID,
		"globalmount",
	)
}

func isLikelyStagingPath(path string) bool {
	return filepath.Base(path) == "globalmount"
}

func (ns *NodeServer) getStagingPathForPublish(ctx context.Context, volumeID string) string {
	if sv := ns.getStagedVolume(volumeID); sv != nil {
		return sv.stagingPath
	}
	if state := ns.findStagingMountState(ctx, volumeID); state != nil {
		if state.StagingPath != "" {
			return state.StagingPath
		}
		return state.TargetPath
	}
	return ns.getDefaultStagingPath(volumeID)
}

func (ns *NodeServer) findStagingMountState(ctx context.Context, volumeID string) *MountState {
	if ns.mountStateManager == nil {
		return nil
	}
	states, err := ns.mountStateManager.LoadState(ctx)
	if err != nil {
		klog.Warningf("Failed to load mount state for staged volume %s: %v", volumeID, err)
		return nil
	}
	for _, state := range states {
		if state.VolumeID != volumeID {
			continue
		}
		if isLikelyStagingPath(state.StagingPath) || isLikelyStagingPath(state.TargetPath) {
			return state
		}
	}
	return nil
}

func (ns *NodeServer) rebuildOrRestage(
	ctx context.Context, req *csi.NodePublishVolumeRequest, stagingPath string,
) error {
	if healthy, _ := ns.stageMountHealthy(stagingPath); healthy {
		return ns.rebuildStagedVolumeFromMount(ctx, req, stagingPath)
	}
	if err := ns.cleanupUnhealthyStagingMount(stagingPath); err != nil {
		return status.Errorf(codes.Internal, "failed to cleanup staging mount before restage: %v", err)
	}
	return ns.stageVolume(ctx, nodeStageRequestFromPublish(req, stagingPath))
}

func (ns *NodeServer) rebuildStagedVolumeFromMount(
	_ context.Context, req *csi.NodePublishVolumeRequest, stagingPath string,
) error {
	volumeID := req.GetVolumeId()
	if healthy, msg := ns.stageMountHealthy(stagingPath); !healthy {
		return status.Errorf(codes.FailedPrecondition, "staging mount %s is not healthy: %s", stagingPath, msg)
	}
	ns.setStagedVolume(volumeID, &stagedVolume{
		volumeID:    volumeID,
		stagingPath: stagingPath,
		mountCtx:    ns.getMountContext(stagingPath),
		readOnly:    req.GetReadonly(),
	})
	klog.V(2).Infof("Rebuilt staged volume cache for %s at %s", volumeID, stagingPath)
	return nil
}

func nodeStageRequestFromPublish(req *csi.NodePublishVolumeRequest, stagingPath string) *csi.NodeStageVolumeRequest {
	return &csi.NodeStageVolumeRequest{
		VolumeId:          req.GetVolumeId(),
		StagingTargetPath: stagingPath,
		VolumeCapability:  req.GetVolumeCapability(),
		Secrets:           req.GetSecrets(),
		VolumeContext:     req.GetVolumeContext(),
	}
}

func (ns *NodeServer) bindPublish(stagingPath, targetPath string, readOnly bool) error {
	options := []string{"bind"}
	if readOnly {
		options = append(options, "ro")
	}
	if err := ns.mounter.Mount(stagingPath, targetPath, "", options); err != nil {
		return status.Errorf(codes.Internal, "failed to bind mount staging path: %v", err)
	}
	return nil
}

func (ns *NodeServer) unbindPublish(targetPath string) error {
	return ns.forceCleanupMount(targetPath)
}

func (ns *NodeServer) rebuildStagedVolumeAfterRemount(
	state *MountState, stagingPath string,
) error {
	ns.setStagedVolume(state.VolumeID, &stagedVolume{
		volumeID:    state.VolumeID,
		stagingPath: stagingPath,
		mountCtx:    ns.getMountContext(stagingPath),
	})
	if err := ns.refreshPublishBindsForVolume(stagingPath, state.VolumeID); err != nil {
		return fmt.Errorf("refresh publish binds: %w", err)
	}
	return nil
}

func (ns *NodeServer) refreshPublishBinds(stagingPath string) error {
	return ns.refreshPublishBindsForVolume(stagingPath, "")
}

func (ns *NodeServer) refreshPublishBindsForVolume(stagingPath, volumeID string) error {
	stagingPath = filepath.Clean(stagingPath)
	stagingComparePath := canonicalMountPath(stagingPath)
	stagingVolumeNames := stagingVolumeNameSet(stagingPath, volumeID)

	// Discover bind refs from the mount table only. Do not call GetMountRefs /
	// PathExists / EvalSymlinks on stagingPath: those Stat the live rclone FUSE
	// from this same process and deadlock (request_wait_answer vs fuse_dev_do_read),
	// especially with vfs-cache-mode=writes. RemountAllStates runs before gRPC
	// listen, so that hang blocks CSI socket forever.
	mounts, err := ns.mounter.List()
	if err != nil {
		return fmt.Errorf("list mounts: %w", err)
	}
	mountByPath := make(map[string]struct {
		source  string
		options []string
	}, len(mounts))
	for _, mp := range mounts {
		path := canonicalMountPath(mp.Path)
		mountByPath[path] = struct {
			source  string
			options []string
		}{
			source:  canonicalMountPath(mp.Device),
			options: append([]string(nil), mp.Opts...),
		}
	}
	stagingMount, stagingMounted := mountByPath[stagingComparePath]

	// Bind mounts share the staging mount's Device in /proc (and FakeMounter).
	refTargets := make(map[string]struct{}, len(mountByPath))
	if stagingMounted {
		for path, mp := range mountByPath {
			if path != stagingComparePath && mp.source == stagingMount.source {
				refTargets[path] = struct{}{}
			}
		}
	}
	for path, mp := range mountByPath {
		if path != stagingComparePath && mp.source == stagingComparePath {
			refTargets[path] = struct{}{}
		}
	}

	err = filepath.WalkDir(kubeletPodsDir, func(path string, d os.DirEntry, walkErr error) error {
		corruptedByWalk := false
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			if filepath.Base(path) != csiPublishMountDir {
				klog.V(4).InfoS("skipping path during publish bind refresh", "path", path, "err", walkErr)
				return nil
			}
			corruptedByWalk = mount.IsCorruptedMnt(walkErr)
			if !corruptedByWalk {
				klog.V(4).InfoS("skipping path during publish bind refresh", "path", path, "err", walkErr)
				return nil
			}
		}
		if walkErr == nil && (!d.IsDir() || filepath.Base(path) != csiPublishMountDir) {
			return nil
		}

		_, volumeName, ok := parseCSIPublishMountPath(path)
		if !ok {
			return nil
		}

		target := filepath.Clean(path)
		targetComparePath := canonicalMountPath(target)
		mp, mounted := mountByPath[targetComparePath]
		_, sourceRef := refTargets[targetComparePath]
		_, sameVolume := stagingVolumeNames[volumeName]
		sourceMatches := sourceRef || (mounted && mp.source == stagingComparePath) ||
			(sameVolume && mounted && stagingMounted && mp.source == stagingMount.source)
		corrupted, _ := IsMountPathCorrupted(target)
		corrupted = corrupted || corruptedByWalk
		if !sourceMatches && (!corrupted || !sameVolume) {
			return nil
		}

		readOnly := mountOptionsReadOnly(mp.options)
		if err := ns.unbindPublish(target); err != nil {
			return fmt.Errorf("unbind publish %s: %w", target, err)
		}
		if err := os.MkdirAll(target, 0755); err != nil {
			return fmt.Errorf("recreate publish target %s: %w", target, err)
		}
		if err := ns.bindPublish(stagingPath, target, readOnly); err != nil {
			return fmt.Errorf("bind publish %s from %s: %w", target, stagingPath, err)
		}
		klog.Infof("Refreshed publish bind %s from staging path %s", target, stagingPath)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk kubelet publish paths: %w", err)
	}
	return nil
}

func stagingVolumeNameSet(stagingPath, volumeID string) map[string]struct{} {
	names := map[string]struct{}{}
	volSuffix := strings.TrimPrefix(volumeID[strings.LastIndex(volumeID, "#")+1:], "#")
	for _, name := range []string{extractVolumeID(stagingPath), volumeID, volSuffix} {
		if name != "" && name != unknownValue {
			names[name] = struct{}{}
		}
	}
	return names
}

func canonicalMountPath(path string) string {
	clean := filepath.Clean(path)
	// Resolve symlinks in the parent only. Never EvalSymlinks/Stat the final
	// component: if it is a live rclone FUSE mount owned by this process,
	// GETATTR deadlocks remount (request_wait_answer vs fuse_dev_do_read).
	dir, base := filepath.Dir(clean), filepath.Base(clean)
	if dir == "." || dir == string(filepath.Separator) {
		return clean
	}
	realDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return clean
	}
	return filepath.Join(realDir, base)
}

func parseCSIPublishMountPath(path string) (podUID, volumeName string, ok bool) {
	parts := strings.Split(filepath.Clean(path), string(os.PathSeparator))
	podsIdx := -1
	csiIdx := -1
	for i, part := range parts {
		switch part {
		case "pods":
			podsIdx = i
		case "kubernetes.io~csi":
			csiIdx = i
		}
	}
	if podsIdx < 0 || csiIdx < 0 || podsIdx+1 >= len(parts) || csiIdx+1 >= len(parts) {
		return "", "", false
	}
	if parts[len(parts)-1] != csiPublishMountDir {
		return "", "", false
	}
	podUID = parts[podsIdx+1]
	volumeName = parts[csiIdx+1]
	return podUID, volumeName, podUID != "" && volumeName != ""
}

func mountOptionsReadOnly(options []string) bool {
	for _, opt := range options {
		if opt == "ro" || strings.HasPrefix(opt, "ro,") || strings.HasSuffix(opt, ",ro") || strings.Contains(opt, ",ro,") {
			return true
		}
	}
	return false
}
