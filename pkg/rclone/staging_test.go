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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
)

func TestDriverStagingCapability(t *testing.T) {
	d := NewDriver(&DriverOptions{DriverName: DefaultDriverName, NodeID: "n1", Staging: true})
	hasStage := false
	for _, c := range d.nscap {
		if c.GetRpc().Type == csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME {
			hasStage = true
		}
	}
	assert.True(t, hasStage)
}

func TestDriverStagingCapabilityDisabled(t *testing.T) {
	d := NewDriver(&DriverOptions{DriverName: DefaultDriverName, NodeID: "n1", Staging: false})
	for _, c := range d.nscap {
		assert.NotEqual(t, csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME, c.GetRpc().Type)
	}
}

func TestStagedVolumeCache(t *testing.T) {
	ns := &NodeServer{}

	volumeID := "vol-1"
	assert.Nil(t, ns.getStagedVolume(volumeID))

	sv := &stagedVolume{
		volumeID:    volumeID,
		stagingPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/pv/vol-1/globalmount",
		readOnly:    false,
	}
	ns.setStagedVolume(volumeID, sv)

	got := ns.getStagedVolume(volumeID)
	assert.NotNil(t, got)
	assert.Equal(t, volumeID, got.volumeID)
	assert.Equal(t, sv.stagingPath, got.stagingPath)

	ns.deleteStagedVolume(volumeID)
	assert.Nil(t, ns.getStagedVolume(volumeID))
}
