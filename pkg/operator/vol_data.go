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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	mount "k8s.io/mount-utils"
)

const volDataFileName = "vol_data.json"

// ReadCSIDriverName reads kubelet vol_data.json next to a CSI volume dir
// (.../kubernetes.io~csi/{volume}/vol_data.json) and returns driverName.
func ReadCSIDriverName(volumeDir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(volumeDir, volDataFileName))
	if err != nil {
		return "", err
	}
	var fields map[string]string
	if err := json.Unmarshal(data, &fields); err != nil {
		return "", fmt.Errorf("parse %s: %w", volDataFileName, err)
	}
	driver := fields["driverName"]
	if driver == "" {
		return "", fmt.Errorf("%s missing driverName", volDataFileName)
	}
	return driver, nil
}

// CSIMountManagedBy reports whether mountPath belongs to provisioner using vol_data.json.
// known is false when vol_data.json is missing or unreadable.
func CSIMountManagedBy(mountPath, provisioner string) (managed, known bool) {
	if provisioner == "" {
		return true, true
	}
	driver, err := ReadCSIDriverName(filepath.Dir(mountPath))
	if err != nil {
		return false, false
	}
	return driver == provisioner, true
}

// PodHasLocalProvisionerVolume checks kubelet CSI volume dirs for the pod UID.
// known is false when the pods CSI dir cannot be listed (other than NotExist).
func PodHasLocalProvisionerVolume(kubeletDir, podUID, provisioner string) (has, known bool) {
	if provisioner == "" || podUID == "" {
		return false, true
	}
	csiDir := filepath.Join(kubeletDir, "pods", podUID, "volumes", csiVolumeDirSegment)
	entries, err := os.ReadDir(csiDir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, true
		}
		return false, false
	}
	unreadable := false
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		driver, err := ReadCSIDriverName(filepath.Join(csiDir, e.Name()))
		if err != nil {
			unreadable = true
			continue
		}
		if driver == provisioner {
			return true, true
		}
	}
	return false, !unreadable
}

// IsRcloneFUSEMount reports whether path is currently mounted as fuse.rclone.
func IsRcloneFUSEMount(mounter mount.Interface, path string) bool {
	if mounter == nil {
		return false
	}
	clean := filepath.Clean(path)
	mounts, err := mounter.List()
	if err != nil {
		return false
	}
	for _, m := range mounts {
		if filepath.Clean(m.Path) != clean {
			continue
		}
		fstype := strings.ToLower(m.Type)
		if fstype == "fuse.rclone" || strings.Contains(fstype, "rclone") {
			return true
		}
	}
	return false
}
