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
)

func TestReadCSIDriverName(t *testing.T) {
	dir := t.TempDir()
	payload := []byte(`{"driverName":"rclone.csi.veloxpack.io"}`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, volDataFileName), payload, 0o644))
	got, err := ReadCSIDriverName(dir)
	require.NoError(t, err)
	assert.Equal(t, "rclone.csi.veloxpack.io", got)
}

func TestCSIMountManagedBy(t *testing.T) {
	mountPath := filepath.Join(t.TempDir(), "mount")
	require.NoError(t, os.MkdirAll(filepath.Dir(mountPath), 0o755))
	require.NoError(t, writeVolData(filepath.Dir(mountPath), "rclone.csi.veloxpack.io"))

	managed, known := CSIMountManagedBy(mountPath, "rclone.csi.veloxpack.io")
	assert.True(t, known)
	assert.True(t, managed)

	managed, known = CSIMountManagedBy(mountPath, "ebs.csi.aws.com")
	assert.True(t, known)
	assert.False(t, managed)

	missing := filepath.Join(t.TempDir(), "mount")
	managed, known = CSIMountManagedBy(missing, "rclone.csi.veloxpack.io")
	assert.False(t, known)
	assert.False(t, managed)
}

func TestPodHasLocalProvisionerVolume(t *testing.T) {
	kubeletDir := t.TempDir()
	podUID := "pod-uid"
	volDir := filepath.Join(kubeletDir, "pods", podUID, "volumes", csiVolumeDirSegment, "data")
	require.NoError(t, os.MkdirAll(volDir, 0o755))
	require.NoError(t, writeVolData(volDir, "rclone.csi.veloxpack.io"))

	has, known := PodHasLocalProvisionerVolume(kubeletDir, podUID, "rclone.csi.veloxpack.io")
	assert.True(t, known)
	assert.True(t, has)

	has, known = PodHasLocalProvisionerVolume(kubeletDir, podUID, "other.driver")
	assert.True(t, known)
	assert.False(t, has)

	has, known = PodHasLocalProvisionerVolume(kubeletDir, "missing", "rclone.csi.veloxpack.io")
	assert.True(t, known)
	assert.False(t, has)
}
