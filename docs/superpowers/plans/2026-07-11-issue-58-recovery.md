# Issue #58 Mount Recovery Implementation Plan

> **For agentic workers:** Implement phases sequentially: A → (B ∥ C). Phase B and C branch from Phase A.

**Goal:** Recover from stale rclone FUSE mounts after CSI driver OOM/restart, with OOM mitigation, driver self-healing, persisted remount-on-boot, and optional pod-restart operator.

**Architecture:** Phase A fixes publish-path idempotency and stale-mount detection in `NodePublishVolume`. Phase B ports `origin/feat/remount` (K8s Secret state + boot remount + graceful shutdown). Phase C adds a node-local DaemonSet operator that detects stale kubelet CSI mount paths and restarts affected workload pods.

**Tech Stack:** Go, CSI gRPC, k8s.io/mount-utils, client-go, controller-runtime (Phase C), Helm

## Global Constraints

- Surgical changes only; match existing style in `pkg/rclone/`
- No commits unless user requests
- Run `go test ./pkg/rclone/...` after each phase
- Phase C operator is optional component under `cmd/volume-recovery-operator/`

---

## Phase A — Foundation (blocks B and C)

**Branch:** `feat/issue-58-phase-a`  
**Worktree:** `.worktrees/phase-a`

### A1. Fix `prepareTargetDirectory` stale-mount detection

**Files:** `pkg/rclone/nodeserver.go`

- [ ] Remove early `return nil` when `!notMnt` before health check
- [ ] When mounted: probe with `os.ReadDir`; if error and `mount.IsCorruptedMnt(err)` → force unmount via `CleanupMountWithForce`
- [ ] Return typed sentinel `errMountAlreadyHealthy` (not empty string) when mount is healthy and accessible
- [ ] Update `NodePublishVolume` to handle sentinel → return success without remounting

### A2. Idempotent `NodePublishVolume` for hibernation overlap

**Files:** `pkg/rclone/nodeserver.go`, `pkg/rclone/utils.go`

- [ ] If mount healthy AND `getMountContext(targetPath) != nil` → return success (idempotent)
- [ ] If mount healthy BUT `mountContext == nil` (post-restart) → cleanup stale entry + remount
- [ ] Replace immediate `TryAcquire` abort with short retry loop (e.g. 3× 500ms) before `Aborted` for overlapping kubelet calls

### A3. Strengthen `isMountHealthy`

**Files:** `pkg/rclone/nodeserver.go`

- [ ] Use `mount.IsCorruptedMnt` on `ReadDir` errors (ENOTCONN detection)
- [ ] Export or duplicate health check as `IsMountPathHealthy(path, mounter)` for Phase C reuse

### A4. Tests

**Files:** `pkg/rclone/nodeserver_test.go`, `pkg/rclone/mount_health_test.go` (new)

- [ ] Test `prepareTargetDirectory` returns sentinel for healthy mount
- [ ] Test corrupted mount triggers cleanup path (fake mounter)
- [ ] Test idempotent publish when mountContext exists

### A5. OOM mitigation docs

**Files:** `charts/values.yaml` (comments), `docs/mount-recovery.md` (new)

- [ ] Document memory vs `vfs-cache-max-size` guidance
- [ ] Document `mountPropagation: HostToContainer` for app pods

**Verify:** `go test ./pkg/rclone/... -count=1`

---

## Phase B — Persisted remount (depends on A)

**Branch:** `feat/issue-58-phase-b` (from `feat/issue-58-phase-a`)  
**Worktree:** `.worktrees/phase-b`

### B1. Port core remount from `origin/feat/remount`

**Files to add:** `pkg/rclone/k8s_client.go`, `pkg/rclone/mount_state.go`  
**Files to modify:** `pkg/rclone/rclone.go`, `pkg/rclone/nodeserver.go`, `cmd/rcloneplugin/main.go`

- [ ] Cherry-pick/adapt `MountState`, `MountStateManager`, `RemountState`, `RemountAllStates`
- [ ] Add `--remount` flag; init state manager when enabled
- [ ] Call `RemountAllStates` on driver boot
- [ ] Save state in `NodePublishVolume` after successful mount
- [ ] Delete state in `NodeUnpublishVolume`

### B2. Fix remount gaps (post-A)

- [ ] `RemountState`: when `mountContext == nil`, always `CleanupMountWithForce` before remount
- [ ] Integrate A's `prepareTargetDirectory` / health checks into `RemountState`

### B3. Graceful shutdown

**Files:** `pkg/rclone/rclone.go`, `cmd/rcloneplugin/main.go`

- [ ] Port SIGTERM handler: unmount all volumes, stop gRPC
- [ ] Optional SIGUSR1 mount dump, SIGUSR2 cache sync from feat/remount

### B4. Deploy / Helm

**Files:** `deploy/components/remount/`, `charts/values.yaml`, `charts/templates/csi-rclone-node.yaml`, RBAC for secrets

- [ ] `remount.enabled` helm value + `--remount` arg
- [ ] `CSI_NAMESPACE` env from downward API
- [ ] Secret RBAC for node SA (create/get/update/delete mount-state secrets)

### B5. Tests

- [ ] Unit tests for `MountState.Validate`, secret name hashing
- [ ] Mock remount flow without real k8s

**Verify:** `go test ./pkg/rclone/... -count=1`

---

## Phase C — Volume recovery operator (depends on A, parallel to B)

**Branch:** `feat/issue-58-phase-c` (from `feat/issue-58-phase-a`)  
**Worktree:** `.worktrees/phase-c`

### C1. Operator scaffold

**Files:** `cmd/volume-recovery-operator/main.go`, `pkg/operator/reconciler.go`

- [ ] DaemonSet binary using in-cluster config
- [ ] `--kubelet-dir` flag (default `/var/lib/kubelet`)
- [ ] `--provisioner` flag (default `rclone.csi.veloxpack.io`)

### C2. Stale mount scanner

**Files:** `pkg/operator/scanner.go`

- [ ] Walk `pods/*/volumes/kubernetes.io~csi/*/mount`
- [ ] Health check using same logic as Phase A (`IsMountPathHealthy`)
- [ ] Map mount path → pod UID + volume name via path parsing

### C3. Pod restart reconciler

**Files:** `pkg/operator/reconciler.go`

- [ ] List pods on this node (field selector `spec.nodeName`)
- [ ] For pods using rclone PVCs with stale mounts: delete pod (owner ref respected via normal k8s recreate)
- [ ] Rate limit: max N restarts per pod per hour (annotation timestamp)
- [ ] Skip kube-system / csi driver pods

### C4. Deploy

**Files:** `deploy/volume-recovery-operator/` (DaemonSet, RBAC, ServiceAccount)

- [ ] hostPath or privileged access to kubelet pods dir
- [ ] Helm optional values under `volumeRecoveryOperator.enabled`

### C5. Docs

**Files:** `docs/mount-recovery.md`

- [ ] Document operator as Phase C complement (app bind-mount recovery)
- [ ] IBM/JuiceFS references

**Verify:** `go test ./pkg/operator/... -count=1`

---

## Integration order

```
main
 └── feat/issue-58-phase-a  (A)
      ├── feat/issue-58-phase-b  (B)
      └── feat/issue-58-phase-c  (C)
```

Merge sequence: A → B → C (or A → B, A → C as separate PRs)

## Review checklist

- [ ] `prepareTargetDirectory` dead code eliminated
- [ ] Hibernation `already exists` mitigated
- [ ] Post-restart remount cleans stale FUSE before remount
- [ ] Operator only restarts workload pods, not CSI pods
- [ ] Docs state: zero-downtime impossible without mountPropagation
