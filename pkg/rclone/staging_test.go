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
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDriverStagingCapability(t *testing.T) {
	d := NewDriver(&DriverOptions{DriverName: DefaultDriverName, NodeID: "n1", Staging: true})
	hasStage := false
	for _, c := range d.nscap {
		if c.GetRpc().Type == csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME {
			hasStage = true
		}
	}
	assert.True(t, hasStage)
}

func TestDriverStagingCapabilityDisabled(t *testing.T) {
	d := NewDriver(&DriverOptions{DriverName: DefaultDriverName, NodeID: "n1", Staging: false})
	for _, c := range d.nscap {
		assert.NotEqual(t, csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME, c.GetRpc().Type)
	}
}

func TestStagedVolumeCache(t *testing.T) {
	ns := &NodeServer{}

	volumeID := "vol-1"
	assert.Nil(t, ns.getStagedVolume(volumeID))

	sv := &stagedVolume{
		volumeID:    volumeID,
		stagingPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/pv/vol-1/globalmount",
		readOnly:    false,
	}
	ns.setStagedVolume(volumeID, sv)

	got := ns.getStagedVolume(volumeID)
	assert.NotNil(t, got)
	assert.Equal(t, volumeID, got.volumeID)
	assert.Equal(t, sv.stagingPath, got.stagingPath)

	ns.deleteStagedVolume(volumeID)
	assert.Nil(t, ns.getStagedVolume(volumeID))
}

func TestFindStagingMountStateUsesStagingPath(t *testing.T) {
	ns, _ := newTestNodeServerWithStaging(t)
	stagingPath := testStagingPath(t)
	publishPath := filepath.Join(t.TempDir(), "pods", "pod-1", "volumes", "kubernetes.io~csi", "vol-stage", "mount")
	require.NoError(t, os.MkdirAll(publishPath, 0755))
	ns.mountStateManager = &MountStateManager{
		namespace: "default",
		nodeID:    "test-node",
		secrets:   &inMemorySecretClient{},
	}
	require.NoError(t, ns.mountStateManager.SaveState(context.Background(), &MountState{
		VolumeID:    "vol-stage",
		StagingPath: stagingPath,
		TargetPath:  publishPath,
		RemoteName:  testRemote,
		MountParams: map[string]string{
			paramRemote:     testRemote,
			paramConfigData: "[s3]\ntype = local\n",
		},
	}))

	state := ns.findStagingMountState(context.Background(), "vol-stage")

	require.NotNil(t, state)
	assert.Equal(t, stagingPath, state.StagingPath)
	assert.Equal(t, stagingPath, ns.getStagingPathForPublish(context.Background(), "vol-stage"))
}

func TestGetDefaultStagingPathUsesDriverName(t *testing.T) {
	volumeID := "vol-driver-name"

	ns := &NodeServer{Driver: NewEmptyDriver("")}
	assert.Equal(t, filepath.Join(
		"/var/lib/kubelet/plugins/kubernetes.io/csi",
		DefaultDriverName,
		"pv",
		volumeID,
		"globalmount",
	), ns.getDefaultStagingPath(volumeID))

	customDriver := "custom.csi.example.io"
	ns.Driver = &Driver{name: customDriver, volumeLocks: NewVolumeLocks()}
	assert.Equal(t, filepath.Join(
		"/var/lib/kubelet/plugins/kubernetes.io/csi",
		customDriver,
		"pv",
		volumeID,
		"globalmount",
	), ns.getDefaultStagingPath(volumeID))
}

func TestRefreshPublishBindsRebindsMountFromStagingPath(t *testing.T) {
	ns, fm := newTestNodeServerWithStaging(t)
	kubeletDir := t.TempDir()
	stagingPath := filepath.Join(
		kubeletDir, "plugins", "kubernetes.io", "csi", DefaultDriverName, "pv", "vol-refresh", "globalmount",
	)
	require.NoError(t, os.MkdirAll(stagingPath, 0755))
	oldPodsDir := kubeletPodsDir
	kubeletPodsDir = filepath.Join(kubeletDir, "pods")
	t.Cleanup(func() { kubeletPodsDir = oldPodsDir })

	publishPath := filepath.Join(kubeletPodsDir, "pod-1", "volumes", "kubernetes.io~csi", "vol-refresh", "mount")
	require.NoError(t, os.MkdirAll(publishPath, 0755))
	require.NoError(t, fm.Mount("test", stagingPath, "", nil))
	require.NoError(t, fm.Mount(stagingPath, publishPath, "", []string{"bind"}))
	fm.mounts = nil
	fm.unmounts = nil

	err := ns.refreshPublishBinds(stagingPath)

	require.NoError(t, err)
	assert.Contains(t, fm.unmounts, publishPath)
	require.Len(t, fm.mounts, 1)
	assert.Equal(t, recordedMount{
		source:  stagingPath,
		target:  publishPath,
		fstype:  "",
		options: []string{"bind"},
	}, fm.mounts[0])
}

func TestRemountStateRebuildsStagingCacheAndRefreshesBindsWhenAlreadyHealthy(t *testing.T) {
	ns, fm := newTestNodeServerWithStaging(t)
	kubeletDir := t.TempDir()
	volumeID := "minio#pvc-healthy-refresh"
	stagingPath := filepath.Join(
		kubeletDir, "plugins", "kubernetes.io", "csi", DefaultDriverName, "pv", "hashed-volume-id", "globalmount",
	)
	publishPath := filepath.Join(
		kubeletDir, "pods", "pod-1", "volumes", "kubernetes.io~csi", "pvc-healthy-refresh", "mount",
	)
	require.NoError(t, os.MkdirAll(stagingPath, 0755))
	require.NoError(t, os.MkdirAll(publishPath, 0755))
	oldPodsDir := kubeletPodsDir
	kubeletPodsDir = filepath.Join(kubeletDir, "pods")
	t.Cleanup(func() { kubeletPodsDir = oldPodsDir })
	require.NoError(t, fm.Mount("test", stagingPath, "", nil))
	require.NoError(t, fm.Mount(stagingPath, publishPath, "", []string{"bind"}))

	mountCalled := false
	ns.mountFilesystem = func(_ string, _ string, _ []string, _ map[string]string) (
		*mountlib.MountPoint, context.Context, context.CancelFunc, error,
	) {
		mountCalled = true
		return nil, nil, nil, nil
	}
	fm.mounts = nil
	fm.unmounts = nil

	err := ns.RemountState(context.Background(), &MountState{
		VolumeID:     volumeID,
		StagingPath:  stagingPath,
		TargetPath:   publishPath,
		ConfigData:   "[remote]\ntype = local\n",
		RemoteName:   "remote",
		MountParams:  map[string]string{},
		MountOptions: []string{},
	})

	require.NoError(t, err)
	assert.False(t, mountCalled)
	assert.NotNil(t, ns.getStagedVolume(volumeID))
	assert.Contains(t, fm.unmounts, publishPath)
	require.Len(t, fm.mounts, 1)
	assert.Equal(t, recordedMount{
		source:  stagingPath,
		target:  publishPath,
		fstype:  "",
		options: []string{"bind"},
	}, fm.mounts[0])
}
