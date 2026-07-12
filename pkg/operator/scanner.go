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
	"strings"

	"github.com/veloxpack/csi-driver-rclone/pkg/rclone"
	mount "k8s.io/mount-utils"
	"k8s.io/klog/v2"
)

const (
	csiVolumeDirSegment = "kubernetes.io~csi"
	mountDirName        = "mount"
)

// StaleMount describes an unhealthy CSI mount discovered on the node.
type StaleMount struct {
	PodUID     string
	VolumeName string
	MountPath  string
}

// ParseCSIMountPath extracts pod UID and volume name from a kubelet CSI mount path.
// Expected layout: .../pods/{podUID}/volumes/kubernetes.io~csi/{volumeName}/mount
func ParseCSIMountPath(path string) (podUID, volumeName string, ok bool) {
	clean := filepath.Clean(path)
	parts := strings.Split(clean, string(os.PathSeparator))

	podsIdx := -1
	csiIdx := -1
	for i, part := range parts {
		switch part {
		case "pods":
			podsIdx = i
		case csiVolumeDirSegment:
			csiIdx = i
		}
	}

	if podsIdx < 0 || csiIdx < 0 || podsIdx+1 >= len(parts) || csiIdx+1 >= len(parts) {
		return "", "", false
	}

	podUID = parts[podsIdx+1]
	volumeName = parts[csiIdx+1]

	if podUID == "" || volumeName == "" {
		return "", "", false
	}
	if parts[len(parts)-1] != mountDirName {
		return "", "", false
	}

	return podUID, volumeName, true
}

// ScanStaleMounts walks kubelet pod volumes and returns CSI mounts that fail health checks.
func ScanStaleMounts(kubeletDir string, mounter mount.Interface) ([]StaleMount, error) {
	podsDir := filepath.Join(kubeletDir, "pods")
	var stale []StaleMount

	err := filepath.WalkDir(podsDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			klog.V(4).InfoS("skipping path during stale mount scan", "path", path, "err", walkErr)
			return nil
		}
		if d.IsDir() || filepath.Base(path) != mountDirName {
			return nil
		}

		podUID, volumeName, ok := ParseCSIMountPath(path)
		if !ok {
			return nil
		}

		corrupted, reason := rclone.IsMountPathCorrupted(path)
		if !corrupted {
			return nil
		}

		klog.V(3).InfoS("found stale CSI mount", "path", path, "podUID", podUID, "volume", volumeName, "reason", reason)
		stale = append(stale, StaleMount{
			PodUID:     podUID,
			VolumeName: volumeName,
			MountPath:  path,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan kubelet mounts under %s: %w", podsDir, err)
	}

	return stale, nil
}
