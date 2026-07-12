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
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rclone/rclone/cmd/mountlib"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

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

func (ns *NodeServer) stageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) error {
	volumeID := req.GetVolumeId()
	stagingPath := req.GetStagingTargetPath()

	if sv := ns.getStagedVolume(volumeID); sv != nil {
		if sv.stagingPath != stagingPath {
			return status.Errorf(codes.FailedPrecondition, "volume %s already staged at %s", volumeID, sv.stagingPath)
		}
		if healthy, msg := ns.isMountHealthy(stagingPath); healthy {
			klog.V(2).Infof("Volume %s already staged at %s", volumeID, stagingPath)
			return nil
		} else {
			klog.Warningf("Staged volume %s at %s is unhealthy: %s", volumeID, stagingPath, msg)
			ns.deleteStagedVolume(volumeID)
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

	if err := ns.prepareTargetDirectory(stagingPath, volumeID); err != nil {
		if !errors.Is(err, errMountAlreadyHealthy) {
			return err
		}
		mc := ns.getMountContext(stagingPath)
		ns.setStagedVolume(volumeID, &stagedVolume{
			volumeID:    volumeID,
			stagingPath: stagingPath,
			mountCtx:    mc,
		})
		klog.V(2).Infof("Rebuilt staged volume cache for %s at %s", volumeID, stagingPath)
		return nil
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
