# Task 3 Report: NodePublishVolume bind mount from staging

## Status

Implemented.

## Changes

- Refactored `NodePublishVolume` into legacy `nodePublishVolumeDirect` and staging `nodePublishVolumeStaged`.
- Added staged publish bind mount flow:
  - resolves cached staging path or mount state path
  - falls back to `/var/lib/kubelet/plugins/kubernetes.io/csi/rclone.csi.veloxpack.io/pv/{volumeID}/globalmount`
  - rebuilds/restages on cache miss or unhealthy cached staging mount
  - prepares publish target as bind target
  - binds staging path to publish target with `bind` and optional `ro`
- Added `bindPublish` and `unbindPublish`.
- Updated staging-mode `NodeUnpublishVolume` to unbind publish target without deleting staging state.
- Updated `NodeGetVolumeStats` health check to resolve staging health from `VolumeId` in staging mode.
- Added `TestNodePublishVolumeBindFromStaging` with recorded fake mounter mount calls.

## Verification

- Red test first:
  - `go test ./pkg/rclone -run TestNodePublishVolumeBindFromStaging -count=1`
  - failed before implementation while publish still attempted direct mount.
- Focused test:
  - `go test ./pkg/rclone -run TestNodePublishVolumeBindFromStaging -count=1`
  - passed.
- Required suite:
  - `go test ./pkg/rclone/... ./pkg/operator/... -count=1`
  - passed:
    - `ok github.com/veloxpack/csi-driver-rclone/pkg/rclone`
    - `ok github.com/veloxpack/csi-driver-rclone/pkg/operator`

## Concerns

- CSI `NodePublishVolumeRequest` has no staging path, so cache-miss recovery depends on persisted mount state or the kubelet default path convention.
- Read-only bind uses `Mount(..., []string{"bind", "ro"})`; some platforms may require remount-style readonly enforcement outside the fake mounter.
