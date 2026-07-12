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

	mount "k8s.io/mount-utils"
)

// errMountAlreadyHealthy signals that the target path is already mounted and accessible.
var errMountAlreadyHealthy = errors.New("mount already healthy")

// IsMountPathCorrupted reports whether a mount path has a stale/broken FUSE endpoint.
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
