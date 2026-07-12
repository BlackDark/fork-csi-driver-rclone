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
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const containerRemountHelperCmd = "__container-remount"
const containerMoveMountHelperCmd = "__container-move-mount"

// moveMountFFromTree is MOVE_MOUNT_F_FROM_TREE from linux/mount.h (not in older x/sys/unix).
const moveMountFFromTree = 0x8

const inheritedTreeFD = 3

// RunContainerRemountHelper runs container mount repair helpers when invoked as:
//   __container-remount <pid> <mountPoint> <readOnly:true|false>  (setns + move_mount; legacy)
//   __container-move-mount <mountPoint> <readOnly:true|false>     (move_mount only; under nsenter)
func RunContainerRemountHelper(args []string) (handled bool, err error) {
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case containerRemountHelperCmd:
		return true, runContainerRemountSetns(args[1:])
	case containerMoveMountHelperCmd:
		return true, runContainerMoveMount(args[1:])
	default:
		return false, nil
	}
}

func runContainerMoveMount(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("%s: expected 2 args, got %d", containerMoveMountHelperCmd, len(args))
	}
	return moveTreeFDIntoMountPoint(args[0], strings.EqualFold(args[1], "true"))
}

func runContainerRemountSetns(args []string) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if len(args) != 3 {
		return fmt.Errorf("%s: expected 3 args, got %d", containerRemountHelperCmd, len(args))
	}
	pid, err := strconv.Atoi(args[0])
	if err != nil || pid <= 1 {
		return fmt.Errorf("%s: invalid pid %q", containerRemountHelperCmd, args[0])
	}

	nsPath := fmt.Sprintf("/proc/%d/ns/mnt", pid)
	nsFD, err := unix.Open(nsPath, unix.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", nsPath, err)
	}
	defer unix.Close(nsFD)

	if err := unix.Setns(nsFD, unix.CLONE_NEWNS); err != nil {
		return fmt.Errorf("setns pid %d: %w", pid, err)
	}
	return moveTreeFDIntoMountPoint(args[1], strings.EqualFold(args[2], "true"))
}

func moveTreeFDIntoMountPoint(mountPoint string, readOnly bool) error {
	_ = unix.Unmount(mountPoint, unix.MNT_DETACH)
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", mountPoint, err)
	}

	flags := moveMountFFromTree | unix.MOVE_MOUNT_T_EMPTY_PATH
	if err := unix.MoveMount(inheritedTreeFD, "", unix.AT_FDCWD, mountPoint, flags); err != nil {
		return fmt.Errorf("move_mount mount %s: %w", mountPoint, err)
	}
	if readOnly {
		if err := unix.Mount("", mountPoint, "", unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
			return fmt.Errorf("remount ro %s: %w", mountPoint, err)
		}
	}
	return nil
}
