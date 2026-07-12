# Task 5 Report: MountState StagingPath + Boot Remount

## Summary

- Added `MountState.StagingPath` and persisted it under the `stagingPath` secret key.
- Updated `NodeStageVolume` state persistence so staging mode records `StagingPath` and keeps legacy `TargetPath` equal to the staging path.
- Updated boot remount to mount `StagingPath` when present, falling back to `TargetPath` for existing persisted secrets.
- Added deserialization migration so old secrets without `stagingPath` populate it from `targetPath`.
- Updated metrics volume ID extraction for kubelet `pv/<volumeID>/globalmount` staging paths.

## Tests

- `go test ./pkg/rclone -run 'TestMountStateSecretIncludesStagingPath|TestMountStateDeserializeMigratesMissingStagingPath|TestNodeStageVolumePersistsStagingPath|TestRemountStateUsesStagingPath|TestExtractVolumeIDFromStagingPath' -count=1`
- `go test ./pkg/rclone/... ./pkg/operator/... -count=1`
