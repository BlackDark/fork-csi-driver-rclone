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

// stagedVolume tracks a FUSE mount at the node-global staging path for a volume.
type stagedVolume struct {
	volumeID    string
	stagingPath string
	mountCtx    *mountContext
	readOnly    bool
	publishRefs int
}

// getStagedVolume retrieves the staged volume cache entry for volumeID.
func (ns *NodeServer) getStagedVolume(volumeID string) *stagedVolume {
	ns.stagedVolumesMu.RLock()
	defer ns.stagedVolumesMu.RUnlock()
	if sv, ok := ns.stagedVolumes[volumeID]; ok {
		return sv
	}
	return nil
}

// setStagedVolume stores a staged volume cache entry for volumeID.
func (ns *NodeServer) setStagedVolume(volumeID string, sv *stagedVolume) {
	ns.stagedVolumesMu.Lock()
	defer ns.stagedVolumesMu.Unlock()
	if ns.stagedVolumes == nil {
		ns.stagedVolumes = make(map[string]*stagedVolume)
	}
	ns.stagedVolumes[volumeID] = sv
}

// deleteStagedVolume removes the staged volume cache entry for volumeID.
func (ns *NodeServer) deleteStagedVolume(volumeID string) {
	ns.stagedVolumesMu.Lock()
	defer ns.stagedVolumesMu.Unlock()
	delete(ns.stagedVolumes, volumeID)
}
