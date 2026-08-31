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

func TestParseFuseConnIDFromMountinfo(t *testing.T) {
	path := "/var/lib/kubelet/pods/dead-uid/volumes/kubernetes.io~csi/pvc-x/mount"
	content := "123 45 0:47 / " + path + " rw,relatime - fuse.rclone rclone:bucket rw\n" +
		"124 45 8:1 / /var/lib/kubelet rw - ext4 /dev/sda1 rw\n"

	id, err := ParseFuseConnIDFromMountinfo(content, path)
	require.NoError(t, err)
	assert.Equal(t, "47", id)
}

func TestParseFuseConnIDFromMountinfoExactPathOnly(t *testing.T) {
	content := "10 1 0:99 / /other/path rw - fuse.rclone rclone:x rw\n"
	_, err := ParseFuseConnIDFromMountinfo(content, "/var/lib/kubelet/pods/x/volumes/kubernetes.io~csi/v/mount")
	require.Error(t, err)
}

func TestParseFuseConnIDFromMountinfoRejectsNonFuse(t *testing.T) {
	path := "/mnt/data"
	content := "10 1 8:1 / /mnt/data rw - ext4 /dev/sda1 rw\n"
	_, err := ParseFuseConnIDFromMountinfo(content, path)
	require.Error(t, err)
}

func TestParseFuseConnIDFromMountinfoUnescape(t *testing.T) {
	// space encoded as \040
	content := "10 1 0:12 / /mnt/my\\040vol rw - fuse rw\n"
	id, err := ParseFuseConnIDFromMountinfo(content, "/mnt/my vol")
	require.NoError(t, err)
	assert.Equal(t, "12", id)
}

func TestShouldAbortOrphanFuse(t *testing.T) {
	assert.True(t, ShouldAbortOrphanFuse(true, false))
	assert.False(t, ShouldAbortOrphanFuse(true, true))
	assert.False(t, ShouldAbortOrphanFuse(false, false))
}

func TestAbortFuseConnWritesSysfs(t *testing.T) {
	root := t.TempDir()
	prev := fuseConnectionsRoot
	fuseConnectionsRoot = root
	t.Cleanup(func() { fuseConnectionsRoot = prev })

	connDir := filepath.Join(root, "47")
	require.NoError(t, os.MkdirAll(connDir, 0o755))
	abortPath := filepath.Join(connDir, "abort")
	require.NoError(t, os.WriteFile(abortPath, []byte{}, 0o644))

	require.NoError(t, AbortFuseConn("47"))
	data, err := os.ReadFile(abortPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("1"), data)
}

func TestAbortFuseConnRejectsBadID(t *testing.T) {
	err := AbortFuseConn("../escape")
	require.Error(t, err)
}

func TestMatchMountServerCmdline(t *testing.T) {
	path := "/var/lib/kubelet/pods/dead/volumes/kubernetes.io~csi/pvc/mount"
	cmdline := []byte("rclone\x00mount\x00remote:bucket\x00" + path + "\x00")
	assert.True(t, MatchMountServerCmdline(cmdline, path))
	assert.False(t, MatchMountServerCmdline([]byte("rclone\x00mount\x00other\x00"), path))
	assert.False(t, MatchMountServerCmdline([]byte("rclone\x00mount\x00"), path))
	assert.False(t, MatchMountServerCmdline(nil, path))
}

func TestShouldKillOrphanMountProcess(t *testing.T) {
	assert.True(t, ShouldKillOrphanMountProcess(true, false))
	assert.False(t, ShouldKillOrphanMountProcess(true, true))
	assert.False(t, ShouldKillOrphanMountProcess(false, false))
}
