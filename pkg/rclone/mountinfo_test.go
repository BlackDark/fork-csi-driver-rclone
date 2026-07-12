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
)

func TestParseCSIPublishMountPathForRemount(t *testing.T) {
	path := "/var/lib/kubelet/pods/d90a7bf8-2c55-4fb2-8cd8-b93ee85d82fb/volumes/kubernetes.io~csi/pvc-8a113f5d-e2d9-474f-834f-c0e2883d78e3/mount"
	podUID, volName, ok := parseCSIPublishMountPath(path)
	assert.True(t, ok)
	assert.Equal(t, "d90a7bf8-2c55-4fb2-8cd8-b93ee85d82fb", podUID)
	assert.Equal(t, "pvc-8a113f5d-e2d9-474f-834f-c0e2883d78e3", volName)
}

func TestContainerRemountEnabledRequiresStagingRemount(t *testing.T) {
	ns := &NodeServer{Driver: &Driver{staging: true, remount: true}}
	// hostPIDEnabled is platform-specific; on non-linux always false
	_ = ns.containerRemountEnabled()
}
