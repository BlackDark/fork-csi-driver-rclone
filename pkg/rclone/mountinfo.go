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
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const fuseRcloneFSType = "fuse.rclone"

// mountInfoEntry is one line from /proc/[pid]/mountinfo.
type mountInfoEntry struct {
	Device     string
	MountPoint string
	FSType     string
	Options    string
}

func readMountInfo(pid int) ([]mountInfoEntry, error) {
	path := "/proc/self/mountinfo"
	if pid > 0 {
		path = fmt.Sprintf("/proc/%d/mountinfo", pid)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []mountInfoEntry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		entry, ok := parseMountInfoLine(scanner.Text())
		if ok {
			entries = append(entries, entry)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func parseMountInfoLine(line string) (mountInfoEntry, bool) {
	fields := strings.Fields(line)
	// mountinfo: id parent major:minor root mountpoint mountopts ... - fstype source superopts
	if len(fields) < 10 {
		return mountInfoEntry{}, false
	}
	sep := -1
	for i, f := range fields {
		if f == "-" {
			sep = i
			break
		}
	}
	if sep < 0 || sep+2 >= len(fields) {
		return mountInfoEntry{}, false
	}
	mountPoint := fields[4]
	if unquoted, err := strconv.Unquote(mountPoint); err == nil {
		mountPoint = unquoted
	}
	return mountInfoEntry{
		Device:     fields[2],
		MountPoint: mountPoint,
		FSType:     fields[sep+1],
		Options:    fields[5],
	}, true
}

func getMountInfoForPath(path string) (device, fsType string, ok bool) {
	compare := canonicalMountPath(path)
	entries, err := readMountInfo(0)
	if err != nil {
		return "", "", false
	}
	for _, e := range entries {
		if canonicalMountPath(e.MountPoint) == compare {
			return e.Device, e.FSType, true
		}
	}
	return "", "", false
}

func isFuseRcloneFSType(fsType string) bool {
	return fsType == fuseRcloneFSType
}

func isMountCorruptedViaProcRoot(pid int, mountPoint string) bool {
	root := filepath.Join(fmt.Sprintf("/proc/%d/root", pid), mountPoint)
	if corrupted, _ := IsMountPathCorrupted(root); corrupted {
		return true
	}
	return false
}
