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
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	mount "k8s.io/mount-utils"
)

func TestIsMountPathHealthy(t *testing.T) {
	mounter, err := NewFakeMounter()
	assert.NoError(t, err)

	t.Run("healthy directory", func(t *testing.T) {
		dir := t.TempDir()
		healthy, msg := IsMountPathHealthy(dir, mounter)
		assert.True(t, healthy)
		assert.Empty(t, msg)
	})

	t.Run("missing path", func(t *testing.T) {
		healthy, msg := IsMountPathHealthy(t.TempDir()+"/missing", mounter)
		assert.False(t, healthy)
		assert.Contains(t, msg, "not accessible")
	})

	t.Run("corrupted mount error", func(t *testing.T) {
		assert.True(t, mount.IsCorruptedMnt(&os.PathError{Op: "readdir", Path: "/mnt/test", Err: syscall.ENOTCONN}))
	})
}

func TestIsMountPathHealthyNotDirectory(t *testing.T) {
	mounter, err := NewFakeMounter()
	assert.NoError(t, err)

	file := t.TempDir() + "/not-a-dir"
	assert.NoError(t, os.WriteFile(file, []byte("x"), 0644))

	healthy, msg := IsMountPathHealthy(file, mounter)
	assert.False(t, healthy)
	assert.Contains(t, msg, "not accessible")
}
