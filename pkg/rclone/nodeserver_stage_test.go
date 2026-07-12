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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

type recordedMount struct {
	source  string
	target  string
	fstype  string
	options []string
}

type recordingMounter struct {
	*mount.FakeMounter
	mounts []recordedMount
}

func (rm *recordingMounter) Mount(source, target, fstype string, options []string) error {
	rm.mounts = append(rm.mounts, recordedMount{
		source:  source,
		target:  target,
		fstype:  fstype,
		options: append([]string(nil), options...),
	})
	return rm.FakeMounter.Mount(source, target, fstype, options)
}

func newTestNodeServerWithStaging(t *testing.T) (*NodeServer, *recordingMounter) {
	t.Helper()

	d := NewEmptyDriver("")
	d.staging = true
	fm := &recordingMounter{FakeMounter: mount.NewFakeMounter(nil)}
	ns := &NodeServer{
		Driver:  d,
		mounter: fm,
	}
	ns.mountFilesystem = func(_ string, targetPath string, _ []string, _ map[string]string) (*mountlib.MountPoint, context.Context, context.CancelFunc, error) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		return nil, cancelCtx, cancel, fm.Mount("test", targetPath, "", nil)
	}
	return ns, fm
}

func testMountCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
	}
}

func testSecrets() map[string]string {
	return map[string]string{
		paramRemote:     testRemote,
		paramConfigData: "[s3]\ntype = local\n",
	}
}

func TestNodeStageVolumeValidation(t *testing.T) {
	ns, _ := newTestNodeServerWithStaging(t)

	tests := []struct {
		desc string
		req  *csi.NodeStageVolumeRequest
		code codes.Code
	}{
		{
			desc: "Volume ID missing",
			req:  &csi.NodeStageVolumeRequest{},
			code: codes.InvalidArgument,
		},
		{
			desc: "Staging target path missing",
			req: &csi.NodeStageVolumeRequest{
				VolumeId: testVolumeID,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Volume capability missing",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          testVolumeID,
				StagingTargetPath: "/mnt/stage",
			},
			code: codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := ns.NodeStageVolume(context.Background(), test.req)
			assert.Equal(t, test.code, status.Code(err))
		})
	}
}

func TestNodeStageVolumeIdempotent(t *testing.T) {
	ns, _ := newTestNodeServerWithStaging(t)
	stagingPath := t.TempDir()
	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testMountCapability(),
		Secrets:           testSecrets(),
	}

	_, err := ns.NodeStageVolume(context.Background(), req)
	require.NoError(t, err)
	assert.NotNil(t, ns.getStagedVolume("vol-1"))

	_, err = ns.NodeStageVolume(context.Background(), req)
	require.NoError(t, err)
}

func TestNodeStageVolumeRebuildsCacheForHealthyMount(t *testing.T) {
	ns, fm := newTestNodeServerWithStaging(t)
	stagingPath := t.TempDir()
	require.NoError(t, fm.Mount("test", stagingPath, "", nil))

	mountCalled := false
	ns.mountFilesystem = func(_ string, _ string, _ []string, _ map[string]string) (*mountlib.MountPoint, context.Context, context.CancelFunc, error) {
		mountCalled = true
		return nil, nil, nil, nil
	}

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-healthy",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testMountCapability(),
		Secrets:           testSecrets(),
	})

	require.NoError(t, err)
	assert.False(t, mountCalled)
	assert.NotNil(t, ns.getStagedVolume("vol-healthy"))
}

func TestNodeStageVolumeRemountsAfterUnhealthyCachedStage(t *testing.T) {
	ns, fm := newTestNodeServerWithStaging(t)
	stagingPath := t.TempDir()
	require.NoError(t, fm.Mount("test", stagingPath, "", nil))

	ns.setStagedVolume("vol-unhealthy", &stagedVolume{
		volumeID:    "vol-unhealthy",
		stagingPath: stagingPath,
		mountCtx:    &mountContext{remoteName: testRemote},
	})
	ns.setMountContext(stagingPath, &mountContext{remoteName: testRemote})

	stageVolumeHealthCheck = func(_ *NodeServer, _ string) (bool, string) {
		return false, "VFS errors detected: 3"
	}
	t.Cleanup(func() { stageVolumeHealthCheck = nil })

	mountCalled := false
	ns.mountFilesystem = func(_ string, targetPath string, _ []string, _ map[string]string) (*mountlib.MountPoint, context.Context, context.CancelFunc, error) {
		mountCalled = true
		cancelCtx, cancel := context.WithCancel(context.Background())
		return nil, cancelCtx, cancel, fm.Mount("test", targetPath, "", nil)
	}

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-unhealthy",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testMountCapability(),
		Secrets:           testSecrets(),
	})

	require.NoError(t, err)
	assert.True(t, mountCalled)
	assert.NotNil(t, ns.getStagedVolume("vol-unhealthy"))
	assert.NotNil(t, ns.getMountContext(stagingPath))
}

func TestNodePublishVolumeBindFromStaging(t *testing.T) {
	ns, fm := newTestNodeServerWithStaging(t)
	stagingPath := t.TempDir()
	targetPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-bind",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testMountCapability(),
		Secrets:           testSecrets(),
	})
	require.NoError(t, err)

	_, err = ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-bind",
		TargetPath:       targetPath,
		VolumeCapability: testMountCapability(),
		Secrets:          testSecrets(),
	})
	require.NoError(t, err)

	require.Len(t, fm.mounts, 2)
	assert.Equal(t, recordedMount{
		source:  stagingPath,
		target:  targetPath,
		fstype:  "",
		options: []string{"bind"},
	}, fm.mounts[1])
}
