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
	"time"

	"github.com/stretchr/testify/assert"
	mount "k8s.io/mount-utils"
)

func TestIsMountPathCorruptedWithTimeout(t *testing.T) {
	t.Run("hung probe returns within timeout", func(t *testing.T) {
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })

		start := time.Now()
		corrupted, reason := probeMountPathWithTimeout("/unused", 50*time.Millisecond, func(string) (bool, string) {
			<-release
			return false, ""
		})
		elapsed := time.Since(start)
		assert.True(t, corrupted)
		assert.Contains(t, reason, "timed out")
		assert.Less(t, elapsed, 2*time.Second)
	})

	t.Run("fast corrupt result", func(t *testing.T) {
		corrupted, reason := probeMountPathWithTimeout("/p", time.Second, func(string) (bool, string) {
			return true, "mount point corrupted: transport endpoint is not connected"
		})
		assert.True(t, corrupted)
		assert.Contains(t, reason, "transport endpoint")
	})

	t.Run("non-positive timeout uses unbounded probe", func(t *testing.T) {
		corrupted, reason := probeMountPathWithTimeout("/p", 0, func(string) (bool, string) {
			return false, ""
		})
		assert.False(t, corrupted)
		assert.Empty(t, reason)
	})

	t.Run("healthy path", func(t *testing.T) {
		dir := t.TempDir()
		corrupted, reason := IsMountPathCorruptedWithTimeout(dir, time.Second)
		assert.False(t, corrupted)
		assert.Empty(t, reason)
	})
}

func TestProbeMountPathWithTimeoutLimitsHungProbes(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	probe := func(string) (bool, string) {
		started <- struct{}{}
		<-release
		return false, ""
	}

	go func() {
		probeMountPathWithTimeout("/first", 20*time.Millisecond, probe)
	}()
	<-started
	corrupted, _ := probeMountPathWithTimeout("/second", 20*time.Millisecond, probe)
	assert.False(t, corrupted)

	select {
	case <-started:
		t.Fatal("second hung probe started instead of respecting the concurrency limit")
	case <-time.After(50 * time.Millisecond):
	}
}

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
