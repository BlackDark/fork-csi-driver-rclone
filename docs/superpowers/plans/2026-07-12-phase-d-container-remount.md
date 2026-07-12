# Phase D: Container Mount Namespace Remount — Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or executing-plans. Reviewed by subagent 2026-07-12.

**Goal:** After CSI restart, restore container I/O without workload pod restart (fix TC-08) by entering each affected container mount namespace via `setns(2)` and replacing stale FUSE mounts with a fresh bind from recovered staging.

**Branch:** `feat/nodestagevolume-staging`  
**Blocking failure:** TC-08 — host `refreshPublishBinds` succeeds; container I/O still `ENOTCONN`

---

## Verified Assumptions (informaten, 2026-07-12)

| ID | Assumption | Result |
|----|------------|--------|
| A1 | Host publish remount changes device ID after CSI restart | **Holds** (0:1294→0:1268) |
| A2 | Container keeps stale device; plain pod gets ENOTCONN | **Holds** |
| A3 | `mountPropagation: HostToContainer` alone fixes recovery | **Fails** — TC-08 + cluster false-positive I/O |
| A4 | CSI pod without `hostPID` can scan host container PIDs | **Fails** — `/proc` ~62 entries |
| A5 | Host SSH + `nsenter` can enter container mount ns | **Holds** (privileged + SYS_ADMIN) |
| A6 | k3s staging path is hash-based, not `pv/{volumeID}` | **Holds** — MountState secret has correct path |
| A7 | `getDefaultStagingPath()` wrong for k3s cache-miss | **Holds** — must fail-closed to MountState |
| A8 | Manual setns from empty staging dir | **Fails** — need healthy FUSE at staging first |

---

## Root Cause

Linux default `rprivate` propagation: kubelet bind at publish path does not update mounts already inside running container namespaces. Staging + `refreshPublishBinds` heals the **host** only.

Reference: [SeaweedFS #255](https://github.com/seaweedfs/seaweedfs-csi-driver/pull/255), hardened by [#267](https://github.com/seaweedfs/seaweedfs-csi-driver/pull/267).

---

## Architecture

```
1. Enumerate publish paths for volume (walk kubeletPodsDir)
2. Per publish path: capture host mountinfo (device, fstype) BEFORE unmount
3. Host recovery: FUSE remount staging (if needed) + refreshPublishBinds
4. Per publish path: find container PIDs (pod UID from publish path) → setns remount
5. Log per-target success; best-effort (do not fail entire RemountState on single pod miss)
```

### Integration points

| Path | Trigger | After |
|------|---------|-------|
| A | Staging healthy (`errMountAlreadyHealthy`) | `rebuildStagedVolumeAfterRemount` → `refreshPublishBinds` → **container remount** |
| B | Staging dead; full FUSE remount | `RemountState` → `refreshPublishBinds` → **container remount** |

---

## File Map

| File | Action |
|------|--------|
| `pkg/rclone/container_remount_linux.go` | Add — setns + open_tree/move_mount |
| `pkg/rclone/container_remount_other.go` | Add — `!linux` stubs |
| `pkg/rclone/container_remount.go` | Add — types + orchestration |
| `pkg/rclone/mountinfo.go` | Add — parse `/proc/*/mountinfo` |
| `pkg/rclone/staging.go` | Modify — container remount hook; fix staging path fallback |
| `pkg/rclone/nodeserver.go` | Modify — wire into `RemountState` both paths |
| `pkg/rclone/container_remount_test.go` | Add |
| `pkg/rclone/container_remount_integration_test.go` | Add — `integration && linux` |
| `charts/values.yaml` | Add `node.hostPID` (auto true when staging+remount) |
| `charts/templates/csi-rclone-node.yaml` | `spec.hostPID` |
| `deploy/base/csi-rclone-node.yaml` | Mirror |
| `hack/e2e/helm-values-staging.yaml` | `node.hostPID: true` |
| `docs/mount-recovery.md` | Phase D section |
| `docs/integration-test-plan.md` | TC-08 update + TC-09 |

---

## Interfaces

```go
type PublishRemountTarget struct {
    PublishPath string
    PodUID      string
    VolumeName  string
    ReadOnly    bool
    PreDevice   string // major:minor before unbind
    PreFSType   string
}

type ContainerRemounter interface {
    RemountStaleMounts(ctx context.Context, stagingPath string, targets []PublishRemountTarget) error
}
```

Linux: `findContainerPIDsForPod(podUID)`, `findStaleFuseMounts(entries, hint)`, `remountViaSetns(pid, mountPoint, stagingPath, readOnly)`.

Gating: `containerRemountEnabled()` = staging + remount + `hostPID`. If disabled, log warning and skip (do not fail RemountState).

---

## Tasks

### Task 1: Mountinfo + publish enumeration
- Parse mountinfo; `collectPublishRemountTargets` with pre-capture device
- Reuse `parseCSIPublishMountPath` + `refreshPublishBindsForVolume` walk
- **Verify:** unit tests with bind-collapse (`fuse.rclone` at publish path)

### Task 2: Container PID discovery
- `findContainerPIDsForPod` — cgroup v1/v2 pod UID match
- Require `hostPID`; skip gracefully if off
- **Verify:** fake `/proc` fixtures; manual `/proc` count in CSI pod

### Task 3: Linux setns remount core
- Adapt SeaweedFS #255 + #267 (`open_tree`/`move_mount` primary, bind fallback)
- Match stale mounts by device OR `fuse.rclone` + corruption when devices differ (0:1295 scenario)
- **Verify:** `sudo go test -tags=integration ./pkg/rclone/ -run ContainerRemount`

### Task 4: Integrate into RemountState
- Call after both `refreshPublishBindsForVolume` paths (A and B)
- Per-target best-effort errors
- **Verify:** mock remounter in existing remount tests

### Task 5: Fix staging path resolution
- Remove silent `getDefaultStagingPath` fallback in production
- Cache-miss without MountState → error (kubelet must NodeStageVolume)
- **Verify:** fail-closed unit test

### Task 6: Helm `hostPID`
- `node.hostPID: null` → auto true when `staging.enabled && remount.enabled`
- Document security tradeoff
- **Verify:** `helm template` output

### Task 7: E2E values + operator coexistence
- Explicit `node.hostPID: true` in `helm-values-staging.yaml`
- Operator TC-06 remains independent fallback
- **Verify:** TC-06 regression

### Task 8: Documentation
- Update `mount-recovery.md` — remove "HostToContainer sufficient" claims
- Limitations: subPath, open FDs, hostPID, non-Linux, direct-publish

### Task 9: TC-08 / TC-09 cluster validation
- TC-08 re-run with hostPID → expect PASS both writers
- TC-09: hostPID false → fail; upgrade true → pass
- **Gate:** do not merge without informaten evidence

---

## Test Plan

### TC-08 (re-run, expect PASS after Phase D)

| Check | Expected |
|-------|----------|
| CSI restart | Workload pod UIDs unchanged |
| Host | `Successfully remounted`, `Refreshed publish bind` |
| Container | `Remounted container mount` per workload |
| I/O | `e2e-writer` + `e2e-writer-propagated` — no ENOTCONN |

### TC-09 (new)

1. Deploy `hostPID: false` → CSI restart → ENOTCONN (confirms gate)
2. Upgrade `hostPID: true` → CSI restart → I/O OK without workload restart
3. CSI pod `/proc` >> 100 entries

---

## Solvability Summary

| Question | Answer |
|----------|--------|
| **Will Phase D definitely solve TC-08?** | **Not until TC-09 passes on-cluster.** High confidence for standard e2e mounts. |
| **Will it solve 100% of workloads?** | **No.** |
| **Operator pod restart (TC-06)?** | **Yes** — already proven; remains fallback. |

### Should work when

- Linux, staging+remount enabled, `hostPID: true`, privileged CSI
- Standard `volumeMount` at `/data`, no `subPath`
- Staging remount succeeds (backend reachable)
- Cgroup PID discovery works on k3s/containerd

### Will fail when

- `hostPID` disabled or blocked by policy
- `volumeMount.subPath` ([#271](https://github.com/seaweedfs/seaweedfs-csi-driver/issues/271))
- Long-held open FDs on dead mount (apps must reopen)
- Non-Linux nodes
- Direct-publish mode (no staging) — out of scope; Phase D2 or operator
- Staging/FUSE remount itself fails

---

## Task Order

```
Task 1 → 2 → 3 → 4 → (5 ∥ 6) → 7 → 8 → 9
```

**Do not merge without Task 9.**

---

## References

- SeaweedFS [#255](https://github.com/seaweedfs/seaweedfs-csi-driver/pull/255) — setns container remount
- SeaweedFS [#267](https://github.com/seaweedfs/seaweedfs-csi-driver/pull/267) — open_tree/move_mount
- SeaweedFS [#271](https://github.com/seaweedfs/seaweedfs-csi-driver/issues/271) — subPath
- Prior plan: `docs/superpowers/plans/2026-07-12-nodestagevolume-staging.md`
- TC-08 evidence: `docs/integration-test-plan.md`
