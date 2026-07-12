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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

// PublishRemountTarget describes one kubelet publish path needing container-side repair.
type PublishRemountTarget struct {
	PublishPath string
	PodUID      string
	VolumeName  string
	ReadOnly    bool
	PreDevice   string
	PreFSType   string
}

func (ns *NodeServer) containerRemountEnabled() bool {
	if ns.Driver == nil || !ns.Driver.staging || !ns.Driver.remount {
		return false
	}
	return hostPIDEnabled()
}

func (ns *NodeServer) collectPublishRemountTargets(stagingPath, volumeID string) ([]PublishRemountTarget, error) {
	stagingPath = filepath.Clean(stagingPath)
	stagingComparePath := canonicalMountPath(stagingPath)
	stagingVolumeNames := stagingVolumeNameSet(stagingPath, volumeID)

	sourceRefs, err := ns.mounter.GetMountRefs(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("get mount refs for %s: %w", stagingPath, err)
	}
	refTargets := make(map[string]struct{}, len(sourceRefs))
	for _, ref := range sourceRefs {
		refTargets[canonicalMountPath(ref)] = struct{}{}
	}

	mounts, err := ns.mounter.List()
	if err != nil {
		return nil, fmt.Errorf("list mounts: %w", err)
	}
	mountByPath := make(map[string]struct {
		source  string
		options []string
	}, len(mounts))
	for _, mp := range mounts {
		mountByPath[canonicalMountPath(mp.Path)] = struct {
			source  string
			options []string
		}{
			source:  canonicalMountPath(mp.Device),
			options: append([]string(nil), mp.Opts...),
		}
	}
	stagingMount, stagingMounted := mountByPath[stagingComparePath]

	var targets []PublishRemountTarget
	err = filepath.WalkDir(kubeletPodsDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			if filepath.Base(path) != "mount" || !mount.IsCorruptedMnt(walkErr) {
				return nil
			}
		}
		if walkErr == nil && (!d.IsDir() || filepath.Base(path) != "mount") {
			return nil
		}

		podUID, volumeName, ok := parseCSIPublishMountPath(path)
		if !ok {
			return nil
		}

		target := filepath.Clean(path)
		targetComparePath := canonicalMountPath(target)
		mp, mounted := mountByPath[targetComparePath]
		_, sourceRef := refTargets[targetComparePath]
		_, sameVolume := stagingVolumeNames[volumeName]
		sourceMatches := sourceRef || (mounted && mp.source == stagingComparePath) ||
			(sameVolume && mounted && stagingMounted && mp.source == stagingMount.source)
		corrupted, _ := IsMountPathCorrupted(target)
		if walkErr != nil && mount.IsCorruptedMnt(walkErr) {
			corrupted = true
		}
		if !sourceMatches && !(corrupted && sameVolume) {
			return nil
		}

		device, fsType, _ := getMountInfoForPath(target)
		if device == "" && mounted {
			device, fsType, _ = getMountInfoForPath(target)
		}
		targets = append(targets, PublishRemountTarget{
			PublishPath: target,
			PodUID:      podUID,
			VolumeName:  volumeName,
			ReadOnly:    mountOptionsReadOnly(mp.options),
			PreDevice:   device,
			PreFSType:   fsType,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk kubelet publish paths: %w", err)
	}
	return targets, nil
}

func (ns *NodeServer) refreshPublishBindsAndRemountContainers(ctx context.Context, stagingPath, volumeID string) error {
	targets, err := ns.collectPublishRemountTargets(stagingPath, volumeID)
	if err != nil {
		return err
	}
	if err := ns.refreshPublishBindsForVolume(stagingPath, volumeID); err != nil {
		return err
	}
	return ns.remountContainersAfterHostRecovery(ctx, stagingPath, targets)
}

func (ns *NodeServer) remountContainersAfterHostRecovery(_ context.Context, stagingPath string, targets []PublishRemountTarget) error {
	if !ns.containerRemountEnabled() {
		klog.V(2).Info("Skipping container remount: requires staging, remount, and hostPID")
		return nil
	}
	if len(targets) == 0 {
		return nil
	}
	if healthy, msg := ns.stageMountHealthy(stagingPath); !healthy {
		klog.Warningf("Skipping container remount: staging path %s not healthy: %s", stagingPath, msg)
		return nil
	}

	var errs []string
	for _, target := range targets {
		if err := remountStaleFuseInContainers(stagingPath, target); err != nil {
			errs = append(errs, fmt.Sprintf("pod %s publish %s: %v", target.PodUID, target.PublishPath, err))
			klog.Warningf("Container remount failed for pod %s at %s: %v", target.PodUID, target.PublishPath, err)
			continue
		}
		klog.Infof("Remounted container mounts for pod %s (publish %s)", target.PodUID, target.PublishPath)
	}
	if len(errs) > 0 {
		return fmt.Errorf("container remount partial failure: %s", strings.Join(errs, "; "))
	}
	return nil
}
