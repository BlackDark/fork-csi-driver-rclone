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

package operator

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mount "k8s.io/mount-utils"
)

func TestParseCSIMountPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantUID    string
		wantVolume string
		wantOK     bool
	}{
		{
			name:       "valid kubelet path",
			path:       "/var/lib/kubelet/pods/abc-123/volumes/kubernetes.io~csi/data/mount",
			wantUID:    "abc-123",
			wantVolume: "data",
			wantOK:     true,
		},
		{
			name: "valid relative segments",
			path: filepath.Join(
				"var", "lib", "kubelet", "pods", "uid-1", "volumes",
				"kubernetes.io~csi", "pvc-vol", "mount",
			),
			wantUID:    "uid-1",
			wantVolume: "pvc-vol",
			wantOK:     true,
		},
		{
			name:   "missing mount suffix",
			path:   "/var/lib/kubelet/pods/abc-123/volumes/kubernetes.io~csi/data",
			wantOK: false,
		},
		{
			name:   "missing pods segment",
			path:   "/var/lib/kubelet/kubernetes.io~csi/data/mount",
			wantOK: false,
		},
		{
			name:   "missing csi segment",
			path:   "/var/lib/kubelet/pods/abc-123/volumes/data/mount",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uid, vol, ok := ParseCSIMountPath(tt.path)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.wantUID, uid)
				assert.Equal(t, tt.wantVolume, vol)
			}
		})
	}
}

func TestScanStaleMountsSkipsNonCorruptFileMount(t *testing.T) {
	kubeletDir := t.TempDir()
	podUID := "pod-uid-123"
	volumeName := "data"
	mountPath := filepath.Join(kubeletDir, "pods", podUID, "volumes", "kubernetes.io~csi", volumeName, "mount")
	require.NoError(t, os.MkdirAll(filepath.Dir(mountPath), 0o755))
	require.NoError(t, os.WriteFile(mountPath, []byte("x"), 0o644))

	mounter := mount.New("" /* mounterPath */)
	stale, err := ScanStaleMounts(kubeletDir, mounter, "")
	require.NoError(t, err)
	assert.Empty(t, stale)
}

func TestScanStaleMountsSkipsHealthyPaths(t *testing.T) {
	kubeletDir := t.TempDir()
	mountPath := filepath.Join(kubeletDir, "pods", "healthy-pod", "volumes", "kubernetes.io~csi", "data", "mount")
	require.NoError(t, os.MkdirAll(mountPath, 0o755))

	mounter := mount.New("" /* mounterPath */)
	stale, err := ScanStaleMounts(kubeletDir, mounter, "")
	require.NoError(t, err)
	assert.Empty(t, stale)
}

func TestScanStaleMountsDetectsCorruptedDirectoryMount(t *testing.T) {
	kubeletDir := t.TempDir()
	podUID := "pod-uid-corrupt"
	volumeName := "data"
	mountPath := filepath.Join(kubeletDir, "pods", podUID, "volumes", "kubernetes.io~csi", volumeName, "mount")
	require.NoError(t, os.MkdirAll(mountPath, 0o755))
	// Child would only be visited if WalkDir incorrectly descends into the mount dir.
	require.NoError(t, os.WriteFile(filepath.Join(mountPath, "should-not-walk"), []byte("x"), 0o644))

	oldProbe := mountPathCorruptedProbe
	mountPathCorruptedProbe = func(path string) (bool, string) {
		if path == mountPath {
			return true, "mount point corrupted: transport endpoint is not connected"
		}
		return false, ""
	}
	t.Cleanup(func() { mountPathCorruptedProbe = oldProbe })

	stale, err := ScanStaleMounts(kubeletDir, mount.New(""), "")
	require.NoError(t, err)
	require.Len(t, stale, 1)
	assert.Equal(t, podUID, stale[0].PodUID)
	assert.Equal(t, volumeName, stale[0].VolumeName)
	assert.Equal(t, mountPath, stale[0].MountPath)
}

func TestScanStaleMountsHungProbeDoesNotBlockForever(t *testing.T) {
	kubeletDir := t.TempDir()
	mountPath := filepath.Join(kubeletDir, "pods", "hung-pod", "volumes", "kubernetes.io~csi", "data", "mount")
	require.NoError(t, os.MkdirAll(mountPath, 0o755))

	oldProbe := mountPathCorruptedProbe
	mountPathCorruptedProbe = func(string) (bool, string) {
		type result struct {
			corrupted bool
			reason    string
		}
		ch := make(chan result, 1)
		go func() {
			time.Sleep(10 * time.Second)
			ch <- result{}
		}()
		select {
		case res := <-ch:
			return res.corrupted, res.reason
		case <-time.After(40 * time.Millisecond):
			return true, "mount probe timed out after 40ms"
		}
	}
	t.Cleanup(func() { mountPathCorruptedProbe = oldProbe })

	start := time.Now()
	stale, err := ScanStaleMounts(kubeletDir, mount.New(""), "")
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Len(t, stale, 1)
	assert.Equal(t, mountPath, stale[0].MountPath)
	assert.Less(t, elapsed, 2*time.Second)
}

func TestScanStaleMountsPreservesSkipDir(t *testing.T) {
	kubeletDir := t.TempDir()
	mountPath := filepath.Join(kubeletDir, "pods", "skip-pod", "volumes", "kubernetes.io~csi", "data", "mount")
	nested := filepath.Join(mountPath, "nested", "deep")
	require.NoError(t, os.MkdirAll(nested, 0o755))

	var probed []string
	oldProbe := mountPathCorruptedProbe
	mountPathCorruptedProbe = func(path string) (bool, string) {
		probed = append(probed, path)
		return false, ""
	}
	t.Cleanup(func() { mountPathCorruptedProbe = oldProbe })

	stale, err := ScanStaleMounts(kubeletDir, mount.New(""), "")
	require.NoError(t, err)
	assert.Empty(t, stale)
	require.Len(t, probed, 1)
	assert.Equal(t, mountPath, probed[0])
}

func TestScanStaleMountsSkipsForeignProvisionerBeforeProbe(t *testing.T) {
	kubeletDir := t.TempDir()
	ours := filepath.Join(kubeletDir, "pods", "pod-a", "volumes", "kubernetes.io~csi", "rclone-vol", "mount")
	foreign := filepath.Join(kubeletDir, "pods", "pod-b", "volumes", "kubernetes.io~csi", "other-vol", "mount")
	require.NoError(t, os.MkdirAll(ours, 0o755))
	require.NoError(t, os.MkdirAll(foreign, 0o755))
	require.NoError(t, writeVolData(filepath.Dir(ours), "rclone.csi.veloxpack.io"))
	require.NoError(t, writeVolData(filepath.Dir(foreign), "ebs.csi.aws.com"))

	var probed []string
	oldProbe := mountPathCorruptedProbe
	mountPathCorruptedProbe = func(path string) (bool, string) {
		probed = append(probed, path)
		return true, "corrupted"
	}
	t.Cleanup(func() { mountPathCorruptedProbe = oldProbe })

	stale, err := ScanStaleMounts(kubeletDir, mount.New(""), "rclone.csi.veloxpack.io")
	require.NoError(t, err)
	require.Len(t, stale, 1)
	assert.Equal(t, ours, stale[0].MountPath)
	assert.Equal(t, []string{ours}, probed)
}

func TestScanStaleMountsProbesOrphanWithoutVolDataWhenFuseRclone(t *testing.T) {
	kubeletDir := t.TempDir()
	orphan := filepath.Join(kubeletDir, "pods", "deadbeef", "volumes", "kubernetes.io~csi", "orphan-vol", "mount")
	foreign := filepath.Join(kubeletDir, "pods", "other", "volumes", "kubernetes.io~csi", "disk", "mount")
	require.NoError(t, os.MkdirAll(orphan, 0o755))
	require.NoError(t, os.MkdirAll(foreign, 0o755))

	fake := &mount.FakeMounter{MountPoints: []mount.MountPoint{
		{Device: "rustfs:bucket", Path: orphan, Type: "fuse.rclone"},
		{Device: "/dev/sda1", Path: foreign, Type: "ext4"},
	}}

	var probed []string
	oldProbe := mountPathCorruptedProbe
	mountPathCorruptedProbe = func(path string) (bool, string) {
		probed = append(probed, path)
		return true, "mount probe timed out"
	}
	t.Cleanup(func() { mountPathCorruptedProbe = oldProbe })

	stale, err := ScanStaleMounts(kubeletDir, fake, "rclone.csi.veloxpack.io")
	require.NoError(t, err)
	require.Len(t, stale, 1)
	assert.Equal(t, orphan, stale[0].MountPath)
	assert.Equal(t, []string{orphan}, probed)
}

func writeVolData(volumeDir, driverName string) error {
	data := []byte(`{"driverName":"` + driverName + `"}`)
	return os.WriteFile(filepath.Join(volumeDir, "vol_data.json"), data, 0o644)
}
