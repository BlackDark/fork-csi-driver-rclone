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
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

const hostPIDProcThreshold = 100

func hostPIDEnabled() bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(entry.Name()); err == nil {
			count++
		}
	}
	return count > hostPIDProcThreshold
}

func findContainerPIDsForPod(podUID string) ([]int, error) {
	if podUID == "" {
		return nil, fmt.Errorf("empty pod UID")
	}
	uidUnderscore := strings.ReplaceAll(podUID, "-", "_")
	markers := []string{
		"pod" + podUID,
		"pod" + uidUnderscore,
		podUID,
		uidUnderscore,
	}

	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}

	seen := map[int]struct{}{}
	var pids []int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 1 {
			continue
		}
		cgroupPath := filepath.Join("/proc", entry.Name(), "cgroup")
		data, err := os.ReadFile(cgroupPath)
		if err != nil {
			continue
		}
		content := string(data)
		matched := false
		for _, marker := range markers {
			if strings.Contains(content, marker) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		pids = append(pids, pid)
	}
	if len(pids) == 0 {
		return nil, fmt.Errorf("no container PIDs found for pod %s", podUID)
	}
	return pids, nil
}

func findStaleContainerMounts(pid int, hint PublishRemountTarget) ([]string, error) {
	entries, err := readMountInfo(pid)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var mountPoints []string
	add := func(mp string) {
		if mp == "" {
			return
		}
		if _, ok := seen[mp]; ok {
			return
		}
		seen[mp] = struct{}{}
		mountPoints = append(mountPoints, mp)
	}
	for _, entry := range entries {
		if !isFuseRcloneFSType(entry.FSType) {
			continue
		}
		match := hint.PreDevice != "" && entry.Device == hint.PreDevice
		if !match && isMountCorruptedViaProcRoot(pid, entry.MountPoint) {
			match = true
		}
		if match {
			add(entry.MountPoint)
		}
	}
	for _, candidate := range []string{"/data"} {
		rootPath := filepath.Join(fmt.Sprintf("/proc/%d/root", pid), candidate)
		if _, err := os.Stat(rootPath); err != nil {
			continue
		}
		if corrupted, _ := IsMountPathCorrupted(rootPath); corrupted {
			add(candidate)
			continue
		}
		// Detached volume: path exists but is no longer a FUSE mount in mountinfo.
		if !mountPointListed(entries, candidate) {
			add(candidate)
		}
	}
	return mountPoints, nil
}

func mountPointListed(entries []mountInfoEntry, mountPoint string) bool {
	for _, entry := range entries {
		if entry.MountPoint == mountPoint && isFuseRcloneFSType(entry.FSType) {
			return true
		}
	}
	return false
}

func remountStaleFuseInContainers(stagingPath string, target PublishRemountTarget) error {
	pids, err := findContainerPIDsForPod(target.PodUID)
	if err != nil {
		return err
	}
	var errs []string
	remounted := false
	for _, pid := range pids {
		mountPoints, err := findStaleContainerMounts(pid, target)
		if err != nil {
			errs = append(errs, fmt.Sprintf("pid %d: %v", pid, err))
			continue
		}
		for _, mountPoint := range mountPoints {
			if err := remountViaSetns(pid, mountPoint, stagingPath, target.ReadOnly); err != nil {
				errs = append(errs, fmt.Sprintf("pid %d mount %s: %v", pid, mountPoint, err))
				continue
			}
			klog.Infof("Remounted container mount pid=%d mount=%s from staging %s", pid, mountPoint, stagingPath)
			remounted = true
		}
	}
	if remounted {
		return nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return fmt.Errorf("no stale fuse mounts found for pod %s", target.PodUID)
}

func remountViaSetns(pid int, mountPoint, stagingPath string, readOnly bool) error {
	treeFD, treeErr := unix.OpenTree(unix.AT_FDCWD, stagingPath, unix.OPEN_TREE_CLONE)
	if treeErr != nil {
		klog.V(4).Infof("open_tree failed for %s, using bind fallback: %v", stagingPath, treeErr)
		return remountViaNsenterBind(pid, mountPoint, stagingPath, readOnly)
	}

	treeFile := os.NewFile(uintptr(treeFD), "tree")
	if treeFile == nil {
		_ = unix.Close(treeFD)
		return fmt.Errorf("wrap open_tree fd for %s", stagingPath)
	}

	helper, err := os.Executable()
	if err != nil {
		_ = treeFile.Close()
		return fmt.Errorf("resolve helper binary: %w", err)
	}

	cmd := exec.Command(
		helper,
		containerRemountHelperCmd,
		strconv.Itoa(pid),
		mountPoint,
		strconv.FormatBool(readOnly),
	)
	cmd.ExtraFiles = []*os.File{treeFile}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("container remount helper pid %d mount %s: %w (%s)", pid, mountPoint, err, strings.TrimSpace(stderr.String()))
	}
	_ = treeFile.Close()
	return nil
}

const hostRemountHelperDir = "/var/lib/kubelet/plugins/kubernetes.io/csi/rclone.csi.veloxpack.io/remount-helper"

var (
	hostRemountHelperOnce sync.Once
	hostRemountHelperPath string
	hostRemountHelperErr  error
)

func containerRemountHelperExecutable() (string, error) {
	hostPath, err := ensureHostRemountHelper()
	if err != nil {
		return "", err
	}
	// Reach host-root binary from workload mnt ns via init proc (requires hostPID).
	return "/proc/1/root" + hostPath, nil
}

func ensureHostRemountHelper() (string, error) {
	hostRemountHelperOnce.Do(func() {
		hostPath := filepath.Join(hostRemountHelperDir, "rcloneplugin")
		exe, err := os.Executable()
		if err != nil {
			hostRemountHelperErr = fmt.Errorf("resolve executable: %w", err)
			return
		}
		if err := os.MkdirAll(hostRemountHelperDir, 0o755); err != nil {
			hostRemountHelperErr = fmt.Errorf("mkdir %s: %w", hostRemountHelperDir, err)
			return
		}
		if _, err := os.Stat(hostPath); err != nil {
			if !os.IsNotExist(err) {
				hostRemountHelperErr = fmt.Errorf("stat %s: %w", hostPath, err)
				return
			}
			if err := installHostRemountHelper(exe, hostPath); err != nil {
				hostRemountHelperErr = err
				return
			}
		}
		hostRemountHelperPath = hostPath
	})
	return hostRemountHelperPath, hostRemountHelperErr
}

func installHostRemountHelper(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open %s: %w", src, err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o555)
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("copy helper to %s: %w", dst, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close %s: %w", dst, err)
	}
	klog.Infof("Installed container remount helper at %s", dst)
	return nil
}

func remountViaNsenterBind(pid int, mountPoint, stagingPath string, readOnly bool) error {
	script := fmt.Sprintf(
		"umount -l %s 2>/dev/null || true; mkdir -p %s; mount --bind %s %s",
		shellQuote(mountPoint), shellQuote(mountPoint), shellQuote(stagingPath), shellQuote(mountPoint),
	)
	if readOnly {
		script += fmt.Sprintf("; mount -o remount,ro %s", shellQuote(mountPoint))
	}

	cmd := exec.Command("nsenter", "-t", strconv.Itoa(pid), "-m", "--", "/bin/sh", "-c", script)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("nsenter bind pid %d mount %s: %w (%s)", pid, mountPoint, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func shellQuote(value string) string {
	return strconv.Quote(value)
}
