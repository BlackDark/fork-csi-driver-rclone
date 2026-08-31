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

import "bytes"

// ShouldKillOrphanMountProcess reports whether best-effort kill of a hung mount
// server is allowed. Only when the pod UID is gone — never for live CSI mounts.
func ShouldKillOrphanMountProcess(orphanKillEnabled, podFound bool) bool {
	return orphanKillEnabled && !podFound
}

// BestEffortKillMountServer signals host processes whose cmdline contains the
// orphan publish path (requires hostPID). SIGKILL for stopped processes.
func BestEffortKillMountServer(path string) error {
	return bestEffortKillMountServer(path)
}

// MatchMountServerCmdline reports whether /proc/*/cmdline (NUL-separated) refers
// to the given publish path. Never matches rclone by name alone.
func MatchMountServerCmdline(cmdline []byte, publishPath string) bool {
	if publishPath == "" || len(cmdline) == 0 {
		return false
	}
	for _, arg := range bytes.Split(cmdline, []byte{0}) {
		if bytes.Equal(arg, []byte(publishPath)) {
			return true
		}
	}
	return false
}
