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
	"errors"
	"fmt"
	"os"
	"time"

	mount "k8s.io/mount-utils"
)

// errMountAlreadyHealthy signals that the target path is already mounted and accessible.
var errMountAlreadyHealthy = errors.New("mount already healthy")

// IsMountPathCorrupted reports whether a mount path has a stale/broken FUSE endpoint.
// This call can block indefinitely on a hung FUSE endpoint; operator code should prefer
// IsMountPathCorruptedWithTimeout.
func IsMountPathCorrupted(path string) (bool, string) {
	_, err := os.ReadDir(path)
	if err == nil {
		return false, ""
	}
	if mount.IsCorruptedMnt(err) {
		return true, fmt.Sprintf("mount point corrupted: %v", err)
	}
	return false, ""
}

// IsMountPathCorruptedWithTimeout is like IsMountPathCorrupted but treats a probe that
// exceeds timeout as corrupted. A non-positive timeout falls back to the unbounded probe.
// On timeout the underlying ReadDir goroutine may remain blocked (uninterruptible FUSE I/O).
func IsMountPathCorruptedWithTimeout(path string, timeout time.Duration) (bool, string) {
	return probeMountPathWithTimeout(path, timeout, IsMountPathCorrupted)
}

// probeMountPathWithTimeout runs probe with a deadline; timeout → corrupted.
func probeMountPathWithTimeout(
	path string, timeout time.Duration, probe func(string) (bool, string),
) (bool, string) {
	if timeout <= 0 {
		return probe(path)
	}

	type result struct {
		corrupted bool
		reason    string
	}
	ch := make(chan result, 1)
	go func() {
		corrupted, reason := probe(path)
		ch <- result{corrupted: corrupted, reason: reason}
	}()

	select {
	case res := <-ch:
		return res.corrupted, res.reason
	case <-time.After(timeout):
		return true, fmt.Sprintf("mount probe timed out after %s", timeout)
	}
}

// IsMountPathHealthy checks whether a mount path is accessible via ReadDir.
// It uses mount.IsCorruptedMnt to detect stale FUSE mounts (e.g. ENOTCONN).
func IsMountPathHealthy(path string, _ mount.Interface) (bool, string) {
	if corrupted, msg := IsMountPathCorrupted(path); corrupted {
		return false, msg
	}
	if _, err := os.ReadDir(path); err != nil {
		return false, fmt.Sprintf("mount point not accessible: %v", err)
	}
	return true, ""
}
