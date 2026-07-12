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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMountStateValidate(t *testing.T) {
	t.Run("nil state", func(t *testing.T) {
		var ms *MountState
		err := ms.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("missing volumeID", func(t *testing.T) {
		ms := &MountState{TargetPath: "/var/lib/kubelet/pods/abc/mount"}
		err := ms.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "volumeID")
	})

	t.Run("missing targetPath", func(t *testing.T) {
		ms := &MountState{VolumeID: "vol-1"}
		err := ms.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "targetPath")
	})

	t.Run("valid state", func(t *testing.T) {
		ms := &MountState{
			VolumeID:   "vol-1",
			TargetPath: "/var/lib/kubelet/pods/abc/mount",
		}
		require.NoError(t, ms.Validate())
	})
}

func TestMountStateManagerMakeSecretName(t *testing.T) {
	sm := &MountStateManager{}

	volumeID := "pvc-12345678-abcd-efgh-ijkl-mnopqrstuvwx"
	name1 := sm.makeSecretName(volumeID)
	name2 := sm.makeSecretName(volumeID)

	assert.Equal(t, name1, name2)
	assert.True(t, len(name1) > len(secretNamePrefix))
	assert.Equal(t, secretNamePrefix, name1[:len(secretNamePrefix)])

	otherName := sm.makeSecretName("other-volume-id")
	assert.NotEqual(t, name1, otherName)
}
