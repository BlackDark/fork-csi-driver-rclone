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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rclone/rclone/cmd/mountlib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// stageVolumeHealthCheck overrides isMountHealthy during stageVolume (tests only).
var stageVolumeHealthCheck func(*NodeServer, string) (bool, string)

func validateStageVolumeRequest(req *csi.NodeStageVolumeRequest) error {
	if len(req.GetVolumeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetStagingTargetPath()) == 0 {
		return status.Error(codes.InvalidArgument, "Staging target path not provided")
	}
	if req.GetVolumeCapability() == nil {
		return status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	return nil
}

func validateUnstageVolumeRequest(req *csi.NodeUnstageVolumeRequest) error {
	if len(req.GetVolumeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetStagingTargetPath()) == 0 {
		return status.Error(codes.InvalidArgument, "Staging target path not provided")
	}
	return nil
}

// NodeStageVolume mounts the rclone filesystem at kubelet's node-global staging path.
//
//nolint:lll
func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if ns.Driver == nil || !ns.Driver.staging {
		return nil, status.Error(codes.Unimplemented, "")
	}
	if err := validateStageVolumeRequest(req); err != nil {
		return nil, err
	}

	volumeID := req.GetVolumeId()
	lockKey := fmt.Sprintf("stage-%s", volumeID)
	release, err := ns.acquireVolumeLock(lockKey, volumeID)
	if err != nil {
		return nil, err
	}
	defer release()

	if err := ns.stageVolume(ctx, req); err != nil {
		return nil, err
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) stageMountHealthy(stagingPath string) (bool, string) {
	if stageVolumeHealthCheck != nil {
		return stageVolumeHealthCheck(ns, stagingPath)
	}
	return ns.isMountHealthy(stagingPath)
}

func (ns *NodeServer) cleanupUnhealthyStagingMount(stagingPath string) error {
	if mc := ns.getMountContext(stagingPath); mc != nil {
		if err := ns.unmountVolume(mc, stagingPath); err != nil {
			klog.Warningf("Failed to unmount unhealthy staging mount at %s: %v", stagingPath, err)
		}
		ns.deleteMountContext(stagingPath)
	}
	if err := ns.forceCleanupMount(stagingPath); err != nil {
		return status.Errorf(codes.Internal, "failed to cleanup unhealthy staging mount: %v", err)
	}
	if err := os.MkdirAll(stagingPath, 0755); err != nil {
		return status.Errorf(codes.Internal, "failed to recreate staging directory after cleanup: %v", err)
	}
	return nil
}

func (ns *NodeServer) stageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) error {
	volumeID := req.GetVolumeId()
	stagingPath := req.GetStagingTargetPath()

	if sv := ns.getStagedVolume(volumeID); sv != nil {
		if sv.stagingPath != stagingPath {
			return status.Errorf(codes.FailedPrecondition, "volume %s already staged at %s", volumeID, sv.stagingPath)
		}
		healthy, healthMsg := ns.stageMountHealthy(stagingPath)
		if healthy {
			klog.V(2).Infof("Volume %s already staged at %s", volumeID, stagingPath)
			return nil
		}
		klog.Warningf("Staged volume %s at %s is unhealthy: %s", volumeID, stagingPath, healthMsg)
		ns.deleteStagedVolume(volumeID)
		if err := ns.cleanupUnhealthyStagingMount(stagingPath); err != nil {
			return err
		}
	}

	if err := ns.prepareTargetDirectory(stagingPath, volumeID); err != nil {
		if !errors.Is(err, errMountAlreadyHealthy) {
			return err
		}
		if healthy, _ := ns.stageMountHealthy(stagingPath); healthy {
			mc := ns.getMountContext(stagingPath)
			ns.setStagedVolume(volumeID, &stagedVolume{
				volumeID:    volumeID,
				stagingPath: stagingPath,
				mountCtx:    mc,
			})
			klog.V(2).Infof("Rebuilt staged volume cache for %s at %s", volumeID, stagingPath)
			return nil
		}
		if err := ns.forceCleanupMount(stagingPath); err != nil {
			return status.Errorf(codes.Internal, "failed to cleanup stale staging mount before remount: %v", err)
		}
		ns.deleteMountContext(stagingPath)
		if err := os.MkdirAll(stagingPath, 0755); err != nil {
			return status.Errorf(codes.Internal, "failed to recreate staging directory after cleanup: %v", err)
		}
	}

	mountCap := req.GetVolumeCapability().GetMount()
	var mountOptions []string
	if mountCap != nil {
		mountOptions = mountCap.GetMountFlags()
	}

	params := ns.mergeVolumeParameters(&csi.NodePublishVolumeRequest{
		VolumeId:         volumeID,
		Secrets:          req.GetSecrets(),
		VolumeContext:    req.GetVolumeContext(),
		VolumeCapability: req.GetVolumeCapability(),
	})

	if mountCap != nil {
		volumeMountGroup := mountCap.GetVolumeMountGroup()
		if volumeMountGroup != "" {
			params[paramVolumeMountGroup] = volumeMountGroup
			params[paramVolumeMountUser] = volumeMountGroup
			params[paramVolumeMountAllowOther] = "true"
			klog.V(2).Infof("Volume mount group: %s, user: %s", volumeMountGroup, params[paramVolumeMountUser])
		}
	}

	pvp, err := extractPublishParams(params)
	if err != nil {
		return err
	}

	klog.V(2).Infof("NodeStageVolume: mounting %s:%s at %s", pvp.remoteName, pvp.remotePath, stagingPath)

	if err := generateConfigData(pvp); err != nil {
		return err
	}

	remotes, err := ns.loadRcloneConfig(ctx, pvp)
	if err != nil {
		return err
	}

	fsPath := buildFsPath(pvp.remoteName, pvp.remotePath)

	var mountSuccess bool
	defer func() {
		if !mountSuccess {
			ns.cleanupConfigRemotes(remotes)
		}
	}()

	mountPoint, mountCtx, cancel, err := ns.mountRcloneFilesystem(fsPath, stagingPath, mountOptions, pvp.params)
	if err != nil {
		return err
	}

	mountSuccess = true
	mc := &mountContext{
		mountPoint: mountPoint,
		remoteName: pvp.remoteName,
		remotes:    remotes,
		cancel:     cancel,
		ctx:        mountCtx,
	}
	ns.setMountContext(stagingPath, mc)
	ns.setStagedVolume(volumeID, &stagedVolume{
		volumeID:    volumeID,
		stagingPath: stagingPath,
		mountCtx:    mc,
	})

	if ns.mountStateManager != nil {
		state := &MountState{
			VolumeID:     volumeID,
			TargetPath:   stagingPath,
			Timestamp:    time.Now(),
			ConfigData:   pvp.configData,
			RemoteName:   pvp.remoteName,
			RemotePath:   pvp.remotePath,
			RemoteType:   pvp.remoteType,
			MountParams:  pvp.params,
			MountOptions: mountOptions,
		}
		if err := ns.mountStateManager.SaveState(ctx, state); err != nil {
			klog.Warningf("Failed to save staging state for volume %s: %v", volumeID, err)
		}
	}

	klog.V(2).Infof("Successfully staged volume %s at %s (remote: %s)", volumeID, stagingPath, pvp.remoteName)
	return nil
}

// NodeUnstageVolume unmounts the rclone filesystem from kubelet's node-global staging path.
//
//nolint:lll
func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if ns.Driver == nil || !ns.Driver.staging {
		return nil, status.Error(codes.Unimplemented, "")
	}
	if err := validateUnstageVolumeRequest(req); err != nil {
		return nil, err
	}

	volumeID := req.GetVolumeId()
	stagingPath := req.GetStagingTargetPath()
	lockKey := fmt.Sprintf("stage-%s", volumeID)
	release, err := ns.acquireVolumeLock(lockKey, volumeID)
	if err != nil {
		return nil, err
	}
	defer release()

	mc := ns.getMountContext(stagingPath)
	if mc == nil {
		if sv := ns.getStagedVolume(volumeID); sv != nil {
			if sv.stagingPath != stagingPath {
				return nil, status.Errorf(codes.FailedPrecondition, "volume %s already staged at %s", volumeID, sv.stagingPath)
			}
			mc = sv.mountCtx
		}
	}

	if err := ns.unmountVolume(mc, stagingPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount staging target %q: %v", stagingPath, err)
	}

	ns.deleteMountContext(stagingPath)
	ns.deleteStagedVolume(volumeID)

	if ns.mountStateManager != nil {
		if err := ns.mountStateManager.DeleteState(ctx, volumeID, stagingPath); err != nil {
			klog.Warningf("Failed to delete staging state for volume %s: %v", volumeID, err)
		}
	}

	klog.V(2).Infof("Successfully unstaged volume %s from %s", volumeID, stagingPath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *NodeServer) mountRcloneFilesystem(
	fsPath, targetPath string,
	mountOptions []string,
	params map[string]string,
) (*mountlib.MountPoint, context.Context, context.CancelFunc, error) {
	if ns.mountFilesystem != nil {
		return ns.mountFilesystem(fsPath, targetPath, mountOptions, params)
	}
	return ns.createAndMountFilesystem(fsPath, targetPath, mountOptions, params)
}
