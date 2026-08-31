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

package operator

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

func bestEffortKillMountServer(path string) error {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return err
	}
	self := os.Getpid()
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 1 || pid == self {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil || len(cmdline) == 0 {
			continue
		}
		if !MatchMountServerCmdline(cmdline, path) {
			continue
		}
		if err := signalMountServer(pid, path); err != nil {
			klog.V(3).InfoS("orphan mount kill signal failed", "pid", pid, "path", path, "err", err)
		}
	}
	return nil
}

func signalMountServer(pid int, path string) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	state, err := processState(pid)
	if err == nil && (state == 'T' || state == 't') {
		if err := proc.Signal(syscall.SIGKILL); err != nil {
			return err
		}
		klog.InfoS("SIGKILL stopped orphan mount process", "pid", pid, "path", path)
		return nil
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	klog.InfoS("SIGTERM orphan mount process", "pid", pid, "path", path)
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !pidAlive(pid) {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !pidAlive(pid) {
		return nil
	}
	if err := proc.Signal(syscall.SIGKILL); err != nil {
		return err
	}
	klog.InfoS("SIGKILL orphan mount process", "pid", pid, "path", path)
	return nil
}

func processState(pid int) (byte, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	i := bytes.LastIndexByte(data, ')')
	if i < 0 || i+2 >= len(data) {
		return 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	return data[i+2], nil
}

func pidAlive(pid int) bool {
	_, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
	return err == nil
}
