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
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
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
	mounts   []recordedMount
	unmounts []string
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

func (rm *recordingMounter) Unmount(target string) error {
	rm.unmounts = append(rm.unmounts, target)
	return rm.FakeMounter.Unmount(target)
}

type inMemorySecretClient struct {
	items map[string]*corev1.Secret
}

func (c *inMemorySecretClient) Create(_ context.Context, secret *corev1.Secret, _ metav1.CreateOptions) (*corev1.Secret, error) {
	if c.items == nil {
		c.items = make(map[string]*corev1.Secret)
	}
	copied := secret.DeepCopy()
	if copied.Data == nil {
		copied.Data = make(map[string][]byte, len(copied.StringData))
	}
	for key, value := range copied.StringData {
		copied.Data[key] = []byte(value)
	}
	c.items[copied.Name] = copied
	return copied.DeepCopy(), nil
}

func (c *inMemorySecretClient) Update(ctx context.Context, secret *corev1.Secret, opts metav1.UpdateOptions) (*corev1.Secret, error) {
	return c.Create(ctx, secret, metav1.CreateOptions{})
}

func (c *inMemorySecretClient) Delete(_ context.Context, name string, _ metav1.DeleteOptions) error {
	delete(c.items, name)
	return nil
}

func (c *inMemorySecretClient) DeleteCollection(context.Context, metav1.DeleteOptions, metav1.ListOptions) error {
	c.items = make(map[string]*corev1.Secret)
	return nil
}

func (c *inMemorySecretClient) Get(_ context.Context, name string, _ metav1.GetOptions) (*corev1.Secret, error) {
	return c.items[name].DeepCopy(), nil
}

func (c *inMemorySecretClient) List(context.Context, metav1.ListOptions) (*corev1.SecretList, error) {
	list := &corev1.SecretList{}
	for _, item := range c.items {
		list.Items = append(list.Items, *item.DeepCopy())
	}
	return list, nil
}

func (c *inMemorySecretClient) Watch(context.Context, metav1.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (c *inMemorySecretClient) Patch(context.Context, string, types.PatchType, []byte, metav1.PatchOptions, ...string) (*corev1.Secret, error) {
	return nil, nil
}

func (c *inMemorySecretClient) Apply(context.Context, *applycorev1.SecretApplyConfiguration, metav1.ApplyOptions) (*corev1.Secret, error) {
	return nil, nil
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

func setTestStagingState(t *testing.T, ns *NodeServer, volumeID, stagingPath string) {
	t.Helper()

	ns.mountStateManager = &MountStateManager{
		namespace: "default",
		nodeID:    "test-node",
		secrets:   &inMemorySecretClient{},
	}
	require.NoError(t, ns.mountStateManager.SaveState(context.Background(), &MountState{
		VolumeID:   volumeID,
		TargetPath: stagingPath,
		Timestamp:  time.Now(),
		RemoteName: testRemote,
		MountParams: map[string]string{
			paramRemote:     testRemote,
			paramConfigData: "[s3]\ntype = local\n",
		},
	}))
}

func testStagingPath(t *testing.T) string {
	t.Helper()

	stagingPath := filepath.Join(t.TempDir(), "globalmount")
	require.NoError(t, os.MkdirAll(stagingPath, 0755))
	return stagingPath
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

func TestNodePublishVolumeRebuildsCacheForHealthyStagingMount(t *testing.T) {
	ns, fm := newTestNodeServerWithStaging(t)
	stagingPath := testStagingPath(t)
	targetPath := t.TempDir()
	setTestStagingState(t, ns, "vol-publish-rebuild", stagingPath)
	require.NoError(t, fm.Mount("test", stagingPath, "", nil))

	mountCalled := false
	ns.mountFilesystem = func(_ string, _ string, _ []string, _ map[string]string) (*mountlib.MountPoint, context.Context, context.CancelFunc, error) {
		mountCalled = true
		return nil, nil, nil, nil
	}

	_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-publish-rebuild",
		TargetPath:       targetPath,
		VolumeCapability: testMountCapability(),
		Secrets:          testSecrets(),
	})
	require.NoError(t, err)

	assert.False(t, mountCalled)
	assert.NotNil(t, ns.getStagedVolume("vol-publish-rebuild"))
	require.Len(t, fm.mounts, 2)
	assert.Equal(t, recordedMount{
		source:  stagingPath,
		target:  targetPath,
		fstype:  "",
		options: []string{"bind"},
	}, fm.mounts[1])
}

func TestNodePublishVolumeRestagesUnhealthyStagingMount(t *testing.T) {
	ns, fm := newTestNodeServerWithStaging(t)
	stagingPath := testStagingPath(t)
	targetPath := t.TempDir()
	setTestStagingState(t, ns, "vol-publish-restage", stagingPath)
	require.NoError(t, fm.Mount("old", stagingPath, "", nil))
	ns.setMountContext(stagingPath, &mountContext{remoteName: "old"})

	stageVolumeHealthCheck = func(_ *NodeServer, path string) (bool, string) {
		if path == stagingPath && ns.getStagedVolume("vol-publish-restage") == nil {
			return false, "VFS errors detected: 3"
		}
		return true, ""
	}
	t.Cleanup(func() { stageVolumeHealthCheck = nil })

	_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:         "vol-publish-restage",
		TargetPath:       targetPath,
		VolumeCapability: testMountCapability(),
		Secrets:          testSecrets(),
	})
	require.NoError(t, err)

	assert.NotNil(t, ns.getStagedVolume("vol-publish-restage"))
	assert.Contains(t, fm.unmounts, stagingPath)
	require.Len(t, fm.mounts, 3)
	assert.Equal(t, recordedMount{
		source:  "test",
		target:  stagingPath,
		fstype:  "",
		options: []string(nil),
	}, fm.mounts[1])
	assert.Equal(t, recordedMount{
		source:  stagingPath,
		target:  targetPath,
		fstype:  "",
		options: []string{"bind"},
	}, fm.mounts[2])
}
