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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rclone/rclone/fs/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

func getTestNodeServer() (NodeServer, error) {
	d := NewEmptyDriver("")
	d.AddNodeServiceCapabilities([]csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_UNKNOWN,
	})
	mounter, err := NewFakeMounter()
	if err != nil {
		return NodeServer{}, errors.New("failed to get fake mounter")
	}
	return NodeServer{
		Driver:  d,
		mounter: mounter,
	}, nil
}

func TestNodeGetInfo(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	req := &csi.NodeGetInfoRequest{}
	resp, err := ns.NodeGetInfo(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, fakeNodeID, resp.NodeId)
}

func TestNodeGetCapabilities(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	req := &csi.NodeGetCapabilitiesRequest{}
	resp, err := ns.NodeGetCapabilities(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Capabilities)
	assert.Equal(t, 1, len(resp.Capabilities))
	assert.Equal(t, csi.NodeServiceCapability_RPC_UNKNOWN, resp.Capabilities[0].GetRpc().Type)
}

func TestNodePublishVolumeValidation(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	tests := []struct {
		desc        string
		req         *csi.NodePublishVolumeRequest
		expectedErr error
	}{
		{
			desc:        "Volume ID missing",
			req:         &csi.NodePublishVolumeRequest{},
			expectedErr: status.Error(codes.InvalidArgument, "Volume ID missing in request"),
		},
		{
			desc: "Target path missing",
			req: &csi.NodePublishVolumeRequest{
				VolumeId: testVolumeID,
			},
			expectedErr: status.Error(codes.InvalidArgument, "Target path not provided"),
		},
		{
			desc: "Volume capability missing",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:   testVolumeID,
				TargetPath: "/mnt/test",
			},
			expectedErr: status.Error(codes.InvalidArgument, "Volume capability missing in request"),
		},
		{
			desc: "Remote parameter missing in volume context",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:   testVolumeID,
				TargetPath: "/mnt/test",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{},
					},
				},
				VolumeContext: map[string]string{},
			},
			expectedErr: status.Error(codes.InvalidArgument, "remote is required (provide via volumeAttributes or secrets)"),
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := ns.NodePublishVolume(context.Background(), test.req)
			if err == nil {
				t.Errorf("Expected error but got nil")
			}
			if status.Code(err) != status.Code(test.expectedErr) {
				t.Errorf("Expected error code %v, got %v (error: %v)", status.Code(test.expectedErr), status.Code(err), err)
			}
		})
	}
}

func TestNodeUnpublishVolumeValidation(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	tests := []struct {
		desc        string
		req         *csi.NodeUnpublishVolumeRequest
		expectedErr error
	}{
		{
			desc:        "Volume ID missing",
			req:         &csi.NodeUnpublishVolumeRequest{},
			expectedErr: status.Error(codes.InvalidArgument, "Volume ID missing in request"),
		},
		{
			desc: "Target path missing",
			req: &csi.NodeUnpublishVolumeRequest{
				VolumeId: testVolumeID,
			},
			expectedErr: status.Error(codes.InvalidArgument, "Target path missing in request"),
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := ns.NodeUnpublishVolume(context.Background(), test.req)
			if err == nil {
				t.Errorf("Expected error but got nil")
			}
			if status.Code(err) != status.Code(test.expectedErr) {
				t.Errorf("Expected error code %v, got %v", status.Code(test.expectedErr), status.Code(err))
			}
		})
	}
}

func TestNodeServerMountContext(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	targetPath := "/mnt/test-volume"

	// Initially, no mount context should exist
	mc := ns.getMountContext(targetPath)
	assert.Nil(t, mc)

	// Create a test mount context
	testMC := &mountContext{
		remoteName: "test-remote",
	}
	ns.setMountContext(targetPath, testMC)

	// Verify it was set
	mc = ns.getMountContext(targetPath)
	assert.NotNil(t, mc)
	assert.Equal(t, "test-remote", mc.remoteName)

	// Delete the mount context
	ns.deleteMountContext(targetPath)

	// Verify it was deleted
	mc = ns.getMountContext(targetPath)
	assert.Nil(t, mc)
}

// TestPrepareTargetDirectoryAlreadyMounted verifies a healthy existing mount returns
// errMountAlreadyHealthy so NodePublishVolume can decide idempotency vs remount.
func TestPrepareTargetDirectoryAlreadyMounted(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	// The fake mounter treats any path containing "false_is_likely" as already mounted.
	base := t.TempDir()
	targetPath := filepath.Join(base, "false_is_likely_mount")
	assert.NoError(t, os.MkdirAll(targetPath, 0755))

	err = ns.prepareTargetDirectory(targetPath, testVolumeID)
	assert.ErrorIs(t, err, errMountAlreadyHealthy)
}

// TestPrepareTargetDirectoryFreshPath verifies a not-yet-mounted target is ready to mount.
func TestPrepareTargetDirectoryFreshPath(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	targetPath := t.TempDir()
	err = ns.prepareTargetDirectory(targetPath, testVolumeID)
	assert.NoError(t, err)
}

// TestMountTimeoutDefault verifies the mount timeout falls back to the default when unset
// and honors an explicit configuration.
func TestMountTimeoutDefault(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	assert.Equal(t, defaultMountTimeout, ns.mountTimeout(), "zero MountTimeout must fall back to the default")

	ns.Driver.mountTimeout = 5 * time.Second
	assert.Equal(t, 5*time.Second, ns.mountTimeout(), "configured MountTimeout must be honored")
}

// TestReapOrphanMountReleasesLockOnFailure verifies that when a handed-off mount ultimately
// fails, the reaper releases the volume lock so future retries can proceed (issue #86).
func TestReapOrphanMountReleasesLockOnFailure(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	lockKey := testVolumeID + "-/mnt/target"
	assert.True(t, ns.Driver.volumeLocks.TryAcquire(lockKey))

	resultCh := make(chan mountResult, 1)
	resultCh <- mountResult{err: errors.New("mount failed")}

	ns.reapOrphanMount(resultCh, filepath.Join(t.TempDir(), "unmounted"), nil, lockKey)

	assert.True(t, ns.Driver.volumeLocks.TryAcquire(lockKey), "reaper must release the lock after a failed orphan mount")
}

// TestReapOrphanMountReleasesLockOnSuccess verifies that when a handed-off mount actually
// came up, the reaper tears it down (bounded) and still releases the lock.
func TestReapOrphanMountReleasesLockOnSuccess(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	lockKey := testVolumeID + "-/mnt/target-ok"
	assert.True(t, ns.Driver.volumeLocks.TryAcquire(lockKey))

	targetPath := t.TempDir()
	cancelled := false
	resultCh := make(chan mountResult, 1)
	resultCh <- mountResult{cancel: func() { cancelled = true }}

	done := make(chan struct{})
	go func() {
		ns.reapOrphanMount(resultCh, targetPath, nil, lockKey)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(gracefulUnmountTimeout + 5*time.Second):
		t.Fatal("reapOrphanMount did not complete within the bounded time")
	}

	assert.True(t, cancelled, "reaper must cancel the mount context before detaching")
	assert.True(t, ns.Driver.volumeLocks.TryAcquire(lockKey),
		"reaper must release the lock after tearing down an orphan mount")
}

func TestPrepareTargetDirectoryErrorSurfaced(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	// "error_is_likely" makes the fake mounter return a generic error.
	targetPath := filepath.Join(t.TempDir(), "error_is_likely")
	assert.NoError(t, os.MkdirAll(targetPath, 0755))

	err = ns.prepareTargetDirectory(targetPath, testVolumeID)
	assert.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestUnimplementedNodeMethods(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	ctx := context.Background()

	// Test NodeStageVolume
	_, err = ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{})
	assert.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))

	// Test NodeUnstageVolume
	_, err = ns.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{})
	assert.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))

	// Test NodeGetVolumeStats
	_, err = ns.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	// Test NodeExpandVolume
	_, err = ns.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{})
	assert.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func newTestNodeServerWithMounter(m mount.Interface) NodeServer {
	d := NewEmptyDriver("")
	d.AddNodeServiceCapabilities([]csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_UNKNOWN,
	})
	return NodeServer{
		Driver:  d,
		mounter: m,
	}
}

func TestPrepareTargetDirectoryHealthyMountReturnsSentinel(t *testing.T) {
	dir := t.TempDir()
	absDir, err := filepath.EvalSymlinks(dir)
	assert.NoError(t, err)
	fm := mount.NewFakeMounter([]mount.MountPoint{{Path: absDir, Device: "test"}})
	ns := newTestNodeServerWithMounter(fm)

	err = ns.prepareTargetDirectory(dir, testVolumeID)
	assert.ErrorIs(t, err, errMountAlreadyHealthy)
}

func TestPrepareTargetDirectoryInaccessibleMountTriggersCleanup(t *testing.T) {
	dir := t.TempDir()
	mountFile := filepath.Join(dir, "mount-target")
	assert.NoError(t, os.WriteFile(mountFile, []byte("x"), 0644))
	absMountFile, err := filepath.EvalSymlinks(mountFile)
	assert.NoError(t, err)

	fm := mount.NewFakeMounter([]mount.MountPoint{{Path: absMountFile, Device: "test"}})
	ns := newTestNodeServerWithMounter(fm)

	err = ns.prepareTargetDirectory(mountFile, testVolumeID)
	assert.NoError(t, err)
	assert.Empty(t, fm.MountPoints)
}

func TestNodePublishVolumeIdempotentWithMountContext(t *testing.T) {
	dir := t.TempDir()
	absDir, err := filepath.EvalSymlinks(dir)
	assert.NoError(t, err)
	fm := mount.NewFakeMounter([]mount.MountPoint{{Path: absDir, Device: "test"}})
	ns := newTestNodeServerWithMounter(fm)
	ns.setMountContext(dir, &mountContext{remoteName: testRemote})

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:   testVolumeID,
		TargetPath: dir,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
		},
		VolumeContext: map[string]string{
			paramRemote:     testRemote,
			paramRemotePath: testRemotePath,
			paramConfigData: "[s3]\ntype = s3\n",
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestAcquireVolumeLockRetry(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	lockKey := "vol-lock-test"
	ns.Driver.volumeLocks.TryAcquire(lockKey)

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(50 * time.Millisecond)
		ns.Driver.volumeLocks.Release(lockKey)
	}()

	release, err := ns.acquireVolumeLock(lockKey, testVolumeID)
	assert.NoError(t, err)
	assert.NotNil(t, release)
	release()
	<-done
}

func TestAcquireVolumeLockExhaustedRetries(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)

	lockKey := "vol-lock-held"
	ns.Driver.volumeLocks.TryAcquire(lockKey)
	defer ns.Driver.volumeLocks.Release(lockKey)

	start := time.Now()
	_, err = ns.acquireVolumeLock(lockKey, testVolumeID)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.GreaterOrEqual(t, elapsed, time.Duration(volumeLockMaxRetries-1)*volumeLockRetryDelay)
}

func TestLegacyPublishUnpublishShareLockKey(t *testing.T) {
	ns, err := getTestNodeServer()
	assert.NoError(t, err)
	assert.False(t, ns.Driver.staging)

	targetPath := filepath.Join(t.TempDir(), "publish-target")
	lockKey := ns.publishVolumeLockKey(testVolumeID, targetPath)
	assert.Equal(t, fmt.Sprintf("%s-%s", testVolumeID, targetPath), lockKey)

	ns.Driver.volumeLocks.TryAcquire(lockKey)

	done := make(chan error, 1)
	go func() {
		_, unpublishErr := ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId:   testVolumeID,
			TargetPath: targetPath,
		})
		done <- unpublishErr
	}()

	select {
	case unpublishErr := <-done:
		t.Fatalf("NodeUnpublishVolume returned before publish lock was released: %v", unpublishErr)
	case <-time.After(50 * time.Millisecond):
	}

	ns.Driver.volumeLocks.Release(lockKey)
	select {
	case unpublishErr := <-done:
		assert.NoError(t, unpublishErr)
	case <-time.After(2 * time.Second):
		t.Fatal("NodeUnpublishVolume did not acquire publish lock after release")
	}
}

func TestPrepareIsolatedConfigDualVolumesNoCollision(t *testing.T) {
	ns, err := getTestNodeServer()
	require.NoError(t, err)

	configData := "[rustfs]\ntype = s3\nprovider = Other\naccess_key_id = KEY\nsecret_access_key = SECRET\n"
	volA := "rustfs#pvc-good"
	volB := "rustfs#pvc-bad"

	pvpA := &publishVolumeParams{remoteName: "rustfs", configData: configData, params: map[string]string{}}
	pvpB := &publishVolumeParams{remoteName: "rustfs", configData: configData, params: map[string]string{}}

	require.NoError(t, ns.prepareIsolatedConfig(volA, pvpA))
	require.NoError(t, ns.prepareIsolatedConfig(volB, pvpB))
	assert.NotEqual(t, pvpA.remoteName, pvpB.remoteName)
	assert.Contains(t, pvpA.remoteName, "rustfs")
	assert.Contains(t, pvpB.remoteName, "rustfs")

	remotesA, err := ns.loadRcloneConfig(context.Background(), pvpA)
	require.NoError(t, err)
	remotesB, err := ns.loadRcloneConfig(context.Background(), pvpB)
	require.NoError(t, err)

	valA, foundA := config.LoadedData().GetValue(pvpA.remoteName, "access_key_id")
	valB, foundB := config.LoadedData().GetValue(pvpB.remoteName, "access_key_id")
	assert.True(t, foundA)
	assert.True(t, foundB)
	assert.Equal(t, "KEY", valA)
	assert.Equal(t, "KEY", valB)
	_, foundLogical := config.LoadedData().GetValue("rustfs", "access_key_id")
	assert.False(t, foundLogical, "logical rustfs section must not be loaded")

	ns.cleanupConfigRemotes(remotesA)
	_, foundA = config.LoadedData().GetValue(pvpA.remoteName, "access_key_id")
	assert.False(t, foundA)
	valB, foundB = config.LoadedData().GetValue(pvpB.remoteName, "access_key_id")
	assert.True(t, foundB)
	assert.Equal(t, "KEY", valB, "cleanup of volume A must not delete volume B section")

	ns.cleanupConfigRemotes(remotesB)
	_, foundB = config.LoadedData().GetValue(pvpB.remoteName, "access_key_id")
	assert.False(t, foundB)
}

func TestPrepareIsolatedConfigDifferentCredsStayIsolated(t *testing.T) {
	ns, err := getTestNodeServer()
	require.NoError(t, err)

	good := "[rustfs]\ntype = s3\naccess_key_id = good\n"
	bad := "[rustfs]\ntype = s3\naccess_key_id = wrong\n"

	pvpGood := &publishVolumeParams{remoteName: "rustfs", configData: good, params: map[string]string{}}
	pvpBad := &publishVolumeParams{remoteName: "rustfs", configData: bad, params: map[string]string{}}

	require.NoError(t, ns.prepareIsolatedConfig("vol-good", pvpGood))
	require.NoError(t, ns.prepareIsolatedConfig("vol-bad", pvpBad))

	_, err = ns.loadRcloneConfig(context.Background(), pvpGood)
	require.NoError(t, err)
	_, err = ns.loadRcloneConfig(context.Background(), pvpBad)
	require.NoError(t, err)
	t.Cleanup(func() {
		ns.cleanupConfigRemotes([]string{pvpGood.remoteName, pvpBad.remoteName})
	})

	goodKey, foundGood := config.LoadedData().GetValue(pvpGood.remoteName, "access_key_id")
	badKey, foundBad := config.LoadedData().GetValue(pvpBad.remoteName, "access_key_id")
	assert.True(t, foundGood)
	assert.True(t, foundBad)
	assert.Equal(t, "good", goodKey)
	assert.Equal(t, "wrong", badKey)
}

func TestExtractPublishParamsRemotePrefixReserved(t *testing.T) {
	pvp, err := extractPublishParams(map[string]string{
		paramRemote:       "rustfs",
		paramConfigData:   "[rustfs]\ntype = s3\n",
		paramRemotePrefix: "shared-ns",
		"vfs-cache-mode":  "writes",
	})
	require.NoError(t, err)
	assert.Equal(t, "shared-ns", pvp.remotePrefix)
	assert.NotContains(t, pvp.params, paramRemotePrefix)
	assert.Equal(t, "writes", pvp.params["vfs_cache_mode"])
}
