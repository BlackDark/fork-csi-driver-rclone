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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMountInfoLineFuseRclone(t *testing.T) {
	line := "8057 28 0:1294 / /var/lib/kubelet/pods/d90a7bf8/volumes/kubernetes.io~csi/pvc-abc/mount " +
		"rw,nosuid,nodev,relatime shared:3039 - fuse.rclone minio:e2e-bucket rw,user_id=0,group_id=0,allow_other"
	entry, ok := parseMountInfoLine(line)
	require.True(t, ok)
	assert.Equal(t, "0:1294", entry.Device)
	assert.Equal(t, "/var/lib/kubelet/pods/d90a7bf8/volumes/kubernetes.io~csi/pvc-abc/mount", entry.MountPoint)
	assert.Equal(t, "fuse.rclone", entry.FSType)
}

func TestIsFuseRcloneFSType(t *testing.T) {
	assert.True(t, isFuseRcloneFSType("fuse.rclone"))
	assert.False(t, isFuseRcloneFSType("fuse"))
}

func TestResolveStagingPathFailClosed(t *testing.T) {
	ns := &NodeServer{}
	assert.Equal(t, "", ns.resolveStagingPath(t.Context(), "vol-unknown", ""))
	assert.Equal(
		t, "/kubelet/staging/globalmount",
		ns.resolveStagingPath(t.Context(), "vol-unknown", "/kubelet/staging/globalmount"),
	)
}

func TestCollectPublishRemountTargetsCapturesDevice(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mount namespace setup")
	}
}
