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
	"context"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

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

func getDefaultStagingPath(volumeID string) string {
	return filepath.Join(
		"/var/lib/kubelet/plugins/kubernetes.io/csi",
		DefaultDriverName,
		"pv",
		volumeID,
		"globalmount",
	)
}

func isLikelyStagingPath(path string) bool {
	return filepath.Base(path) == "globalmount"
}

func (ns *NodeServer) getStagingPathForPublish(ctx context.Context, volumeID string) string {
	if sv := ns.getStagedVolume(volumeID); sv != nil {
		return sv.stagingPath
	}
	if state := ns.findStagingMountState(ctx, volumeID); state != nil {
		return state.TargetPath
	}
	return getDefaultStagingPath(volumeID)
}

func (ns *NodeServer) findStagingMountState(ctx context.Context, volumeID string) *MountState {
	if ns.mountStateManager == nil {
		return nil
	}
	states, err := ns.mountStateManager.LoadState(ctx)
	if err != nil {
		klog.Warningf("Failed to load mount state for staged volume %s: %v", volumeID, err)
		return nil
	}
	for _, state := range states {
		if state.VolumeID == volumeID && isLikelyStagingPath(state.TargetPath) {
			return state
		}
	}
	return nil
}

func (ns *NodeServer) rebuildOrRestage(ctx context.Context, req *csi.NodePublishVolumeRequest, stagingPath string) error {
	if healthy, _ := ns.stageMountHealthy(stagingPath); healthy {
		return ns.rebuildStagedVolumeFromMount(ctx, req, stagingPath)
	}
	if err := ns.cleanupUnhealthyStagingMount(stagingPath); err != nil {
		return status.Errorf(codes.Internal, "failed to cleanup staging mount before restage: %v", err)
	}
	return ns.stageVolume(ctx, nodeStageRequestFromPublish(req, stagingPath))
}

func (ns *NodeServer) rebuildStagedVolumeFromMount(_ context.Context, req *csi.NodePublishVolumeRequest, stagingPath string) error {
	volumeID := req.GetVolumeId()
	if healthy, msg := ns.stageMountHealthy(stagingPath); !healthy {
		return status.Errorf(codes.FailedPrecondition, "staging mount %s is not healthy: %s", stagingPath, msg)
	}
	ns.setStagedVolume(volumeID, &stagedVolume{
		volumeID:    volumeID,
		stagingPath: stagingPath,
		mountCtx:    ns.getMountContext(stagingPath),
		readOnly:    req.GetReadonly(),
	})
	klog.V(2).Infof("Rebuilt staged volume cache for %s at %s", volumeID, stagingPath)
	return nil
}

func nodeStageRequestFromPublish(req *csi.NodePublishVolumeRequest, stagingPath string) *csi.NodeStageVolumeRequest {
	return &csi.NodeStageVolumeRequest{
		VolumeId:          req.GetVolumeId(),
		StagingTargetPath: stagingPath,
		VolumeCapability:  req.GetVolumeCapability(),
		Secrets:           req.GetSecrets(),
		VolumeContext:     req.GetVolumeContext(),
	}
}

func (ns *NodeServer) bindPublish(stagingPath, targetPath string, readOnly bool) error {
	options := []string{"bind"}
	if readOnly {
		options = append(options, "ro")
	}
	if err := ns.mounter.Mount(stagingPath, targetPath, "", options); err != nil {
		return status.Errorf(codes.Internal, "failed to bind mount staging path: %v", err)
	}
	return nil
}

func (ns *NodeServer) unbindPublish(targetPath string) error {
	return ns.forceCleanupMount(targetPath)
}
