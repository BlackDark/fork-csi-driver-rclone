//go:build linux

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
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const containerRemountHelperCmd = "__container-remount"

// moveMountFFromTree is MOVE_MOUNT_F_FROM_TREE from linux/mount.h (not in older x/sys/unix).
const moveMountFFromTree = 0x8

const inheritedTreeFD = 3

// RunContainerRemountHelper runs the single-threaded setns+move_mount helper when invoked
// as: __container-remount <pid> <mountPoint> <readOnly:true|false>
func RunContainerRemountHelper(args []string) (handled bool, err error) {
	if len(args) == 0 || args[0] != containerRemountHelperCmd {
		return false, nil
	}
	if len(args) != 4 {
		return true, fmt.Errorf("%s: expected 3 args, got %d", containerRemountHelperCmd, len(args)-1)
	}
	pid, err := strconv.Atoi(args[1])
	if err != nil || pid <= 1 {
		return true, fmt.Errorf("%s: invalid pid %q", containerRemountHelperCmd, args[1])
	}
	mountPoint := args[2]
	readOnly := strings.EqualFold(args[3], "true")

	nsPath := fmt.Sprintf("/proc/%d/ns/mnt", pid)
	nsFD, err := unix.Open(nsPath, unix.O_RDONLY, 0)
	if err != nil {
		return true, fmt.Errorf("open %s: %w", nsPath, err)
	}
	defer unix.Close(nsFD)

	if err := unix.Setns(nsFD, unix.CLONE_NEWNS); err != nil {
		return true, fmt.Errorf("setns pid %d: %w", pid, err)
	}

	_ = unix.Unmount(mountPoint, unix.MNT_DETACH)
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return true, fmt.Errorf("mkdir %s: %w", mountPoint, err)
	}

	flags := moveMountFFromTree | unix.MOVE_MOUNT_T_EMPTY_PATH
	if err := unix.MoveMount(inheritedTreeFD, "", unix.AT_FDCWD, mountPoint, flags); err != nil {
		return true, fmt.Errorf("move_mount pid %d mount %s: %w", pid, mountPoint, err)
	}
	if readOnly {
		if err := unix.Mount("", mountPoint, "", unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
			return true, fmt.Errorf("remount ro %s: %w", mountPoint, err)
		}
	}
	return true, nil
}
