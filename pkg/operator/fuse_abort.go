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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// fuseConnectionsRoot is the sysfs root for FUSE connections (overridable in tests).
var fuseConnectionsRoot = "/sys/fs/fuse/connections"

// ShouldAbortOrphanFuse reports whether an orphan publish path may have its FUSE
// connection aborted. Only when the pod UID is gone — never for live CSI mounts.
func ShouldAbortOrphanFuse(orphanFuseAbortEnabled, podFound bool) bool {
	return orphanFuseAbortEnabled && !podFound
}

// ResolveFuseConnID returns the FUSE connection id (mountinfo device minor) for path.
func ResolveFuseConnID(path string) (string, error) {
	return resolveFuseConnID(path)
}

// AbortFuseConn writes to /sys/fs/fuse/connections/<id>/abort (best-effort).
func AbortFuseConn(id string) error {
	if _, err := strconv.Atoi(id); err != nil {
		return fmt.Errorf("invalid fuse connection id %q: %w", id, err)
	}
	abortPath := filepath.Join(fuseConnectionsRoot, id, "abort")
	return os.WriteFile(abortPath, []byte("1"), 0o200)
}

// ParseFuseConnIDFromMountinfo finds the FUSE connection id for mountPath in mountinfo content.
// Connection id is the device minor; fstype must be fuse or fuse.*.
func ParseFuseConnIDFromMountinfo(content, mountPath string) (string, error) {
	want := filepath.Clean(mountPath)
	for _, line := range strings.Split(content, "\n") {
		if line == "" {
			continue
		}
		sep := strings.Index(line, " - ")
		if sep < 0 {
			continue
		}
		left := strings.Fields(line[:sep])
		if len(left) < 5 {
			continue
		}
		mountPoint := filepath.Clean(unescapeMountinfo(left[4]))
		if mountPoint != want {
			continue
		}
		right := strings.Fields(line[sep+3:])
		if len(right) < 1 {
			continue
		}
		fstype := right[0]
		if fstype != "fuse" && !strings.HasPrefix(fstype, "fuse.") {
			continue
		}
		majMin := left[2]
		_, minor, ok := strings.Cut(majMin, ":")
		if !ok || minor == "" {
			continue
		}
		if _, err := strconv.Atoi(minor); err != nil {
			continue
		}
		return minor, nil
	}
	return "", fmt.Errorf("fuse mountinfo entry not found for %s", mountPath)
}

// unescapeMountinfo decodes octal escapes used in /proc/*/mountinfo fields.
func unescapeMountinfo(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
