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
			name:       "valid relative segments",
			path:       filepath.Join("var", "lib", "kubelet", "pods", "uid-1", "volumes", "kubernetes.io~csi", "pvc-vol", "mount"),
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
	stale, err := ScanStaleMounts(kubeletDir, mounter)
	require.NoError(t, err)
	assert.Empty(t, stale)
}

func TestScanStaleMountsSkipsHealthyPaths(t *testing.T) {
	kubeletDir := t.TempDir()
	mountPath := filepath.Join(kubeletDir, "pods", "healthy-pod", "volumes", "kubernetes.io~csi", "data", "mount")
	require.NoError(t, os.MkdirAll(mountPath, 0o755))

	mounter := mount.New("" /* mounterPath */)
	stale, err := ScanStaleMounts(kubeletDir, mounter)
	require.NoError(t, err)
	assert.Empty(t, stale)
}
