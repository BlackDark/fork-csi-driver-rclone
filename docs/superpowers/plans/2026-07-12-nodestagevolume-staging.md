# NodeStageVolume Staging Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor rclone CSI node operations to use `NodeStageVolume` (FUSE at node-global staging path) + `NodePublishVolume` (bind mount to pod path), enabling SeaweedFS-style self-healing after driver restart and fixing stale container bind mounts without workload restarts when combined with `mountPropagation: HostToContainer`.

**Architecture:** Move the FUSE mount from per-pod publish paths to a per-volume-per-node staging path (`.../globalmount`). `NodePublishVolume` bind-mounts staging → pod target. In-memory `stagedVolumes` cache (keyed by `volumeID`) holds FUSE `mountContext`; publish paths are plain bind mounts. On cache miss after driver restart, `NodePublishVolume` rebuilds from healthy staging or re-stages (SeaweedFS #212). Persisted `MountState` remounts staging paths on boot (Phase B). Operator becomes fallback only.

**Tech Stack:** Go, CSI gRPC, k8s.io/mount-utils bind mounts, existing rclone mountlib integration, Helm, `hack/e2e/` cluster validation on `informaten`

## Global Constraints

- Branch: `feat/nodestagevolume-staging` from `feat/issue-58-recovery`
- Surgical changes; match `pkg/rclone/` style
- Reuse Phase A health checks (`IsMountPathCorrupted`, `prepareTargetDirectory`, `forceCleanupMount`)
- Reuse Phase B `MountState` persistence; extend schema for staging path
- Keep legacy direct-mount path behind `node.staging.enabled=false` for one release (rollback safety)
- Run `go test ./pkg/rclone/... ./pkg/operator/... -count=1` after each task
- Cluster validation required before claiming done (TC-08 on `informaten`)
- No commits unless user requests (or end-of-task per plan)

---

## Problem Statement

| Current (direct publish) | Target (stage + publish) |
|--------------------------|--------------------------|
| FUSE mounted at `/var/lib/kubelet/pods/{uid}/.../mount` | FUSE at `/var/lib/kubelet/plugins/kubernetes.io/csi/.../globalmount` |
| One FUSE per pod mount | One FUSE per volume per node |
| Driver restart loses `mountContext` at publish path | Staging remount heals all pod bind mounts (with propagation) |
| TC-03b FAIL: workload stale after CSI restart | TC-08 target: workload I/O OK without pod restart |

Reference: [SeaweedFS CSI #212](https://github.com/seaweedfs/seaweedfs-csi-driver/pull/212)

---

## File Map

| File | Responsibility |
|------|----------------|
| `pkg/rclone/staging.go` (new) | `stagedVolume` struct, cache, bind/unbind helpers, staging health |
| `pkg/rclone/nodeserver.go` | Gate legacy vs staging in publish/unpublish; wire self-heal |
| `pkg/rclone/nodeserver_stage.go` (new) | `NodeStageVolume`, `NodeUnstageVolume` implementations |
| `pkg/rclone/mount_state.go` | Add `StagingPath`; migrate `TargetPath` semantics |
| `pkg/rclone/rclone.go` | `DriverOptions.Staging`, conditional `STAGE_UNSTAGE_VOLUME` cap |
| `cmd/rcloneplugin/main.go` | `--staging` flag |
| `charts/values.yaml` + `charts/templates/csi-rclone-node.yaml` | `node.staging.enabled` |
| `pkg/rclone/nodeserver_stage_test.go` (new) | Stage/publish/unstage unit tests |
| `hack/e2e/helm-values-staging.yaml` (new) | E2E values with staging enabled |
| `docs/mount-recovery.md` | Staging architecture section |
| `docs/integration-test-plan.md` | TC-08 staging recovery test |

---

## Interfaces (cross-task contract)

```go
// pkg/rclone/staging.go

type stagedVolume struct {
    volumeID    string
    stagingPath string
    mountCtx    *mountContext  // FUSE lives here
    readOnly    bool
    publishRefs int            // optional ref-count guard
}

// Cache on NodeServer:
// stagedVolumes map[string]*stagedVolume  // key: volumeID

func (ns *NodeServer) getStagedVolume(volumeID string) *stagedVolume
func (ns *NodeServer) setStagedVolume(volumeID string, sv *stagedVolume)
func (ns *NodeServer) deleteStagedVolume(volumeID string)

func (ns *NodeServer) bindPublish(stagingPath, targetPath string, readOnly bool) error
func (ns *NodeServer) unbindPublish(targetPath string) error

func (ns *NodeServer) ensureStaged(ctx context.Context, volumeID, stagingPath string, req mountParams) error
func (ns *NodeServer) rebuildOrRestage(ctx context.Context, volumeID, stagingPath string, req mountParams) error
```

---

### Task 1: Staging scaffold + capability flag

**Files:**
- Create: `pkg/rclone/staging.go`
- Modify: `pkg/rclone/rclone.go`, `pkg/rclone/nodeserver.go`
- Test: `pkg/rclone/staging_test.go`

**Interfaces:**
- Produces: `stagedVolume`, cache helpers, `DriverOptions.Staging bool`

- [ ] **Step 1: Write failing test for capability gating**

```go
func TestDriverStagingCapability(t *testing.T) {
    d := NewDriver(&DriverOptions{DriverName: DefaultDriverName, NodeID: "n1", Staging: true})
    hasStage := false
    for _, c := range d.nscap {
        if c.GetRpc().Type == csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME {
            hasStage = true
        }
    }
    assert.True(t, hasStage)
}
```

- [ ] **Step 2: Run test — expect FAIL**

Run: `go test ./pkg/rclone/... -run TestDriverStagingCapability -count=1`
Expected: FAIL

- [ ] **Step 3: Add `Staging` to `DriverOptions`; add cap when true**

```go
// rclone.go DriverOptions
Staging bool

// NewDriver — after existing caps:
if options.Staging {
    d.AddNodeServiceCapabilities([]csi.NodeServiceCapability_RPC_Type{
        csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
    })
}
```

- [ ] **Step 4: Add `stagedVolumes map` + helpers in `staging.go`**

- [ ] **Step 5: Run tests — expect PASS**

Run: `go test ./pkg/rclone/... -count=1`

---

### Task 2: NodeStageVolume — FUSE at staging path

**Files:**
- Create: `pkg/rclone/nodeserver_stage.go`
- Modify: `pkg/rclone/nodeserver.go` (extract shared mount logic)
- Test: `pkg/rclone/nodeserver_stage_test.go`

**Interfaces:**
- Consumes: `prepareTargetDirectory`, `createAndMountFilesystem`, `mergeVolumeParameters`, `extractPublishParams`
- Produces: `(*NodeServer).NodeStageVolume`, `(*NodeServer).stageVolume(...)`

- [ ] **Step 1: Write failing test**

```go
func TestNodeStageVolumeIdempotent(t *testing.T) {
    ns, _ := newTestNodeServerWithStaging(t)
    stagingPath := t.TempDir()
    req := &csi.NodeStageVolumeRequest{
        VolumeId: "vol-1",
        StagingTargetPath: stagingPath,
        VolumeCapability: testMountCapability(),
        Secrets: testSecrets(),
    }
    _, err := ns.NodeStageVolume(context.Background(), req)
    require.NoError(t, err)
    assert.NotNil(t, ns.getStagedVolume("vol-1"))

    // second call idempotent
    _, err = ns.NodeStageVolume(context.Background(), req)
    require.NoError(t, err)
}
```

- [ ] **Step 2: Run test — expect FAIL** (`Unimplemented`)

- [ ] **Step 3: Implement `NodeStageVolume`**

Logic:
1. Validate `volumeID`, `stagingTargetPath`, `volumeCapability`
2. Acquire lock `stage-{volumeID}`
3. If `getStagedVolume(volumeID)` exists and staging healthy → return success
4. `prepareTargetDirectory(stagingPath)` — heal stale FUSE
5. Extract params from secrets/volumeContext (same as publish)
6. `createAndMountFilesystem(fsPath, stagingPath, ...)`
7. `setStagedVolume(volumeID, &stagedVolume{...})`
8. `SaveState` with `StagingPath` (Task 4 wires field)

- [ ] **Step 4: Handle stale staging in stage path** (SeaweedFS Phase 2)

If `prepareTargetDirectory` returns healthy but no `stagedVolume` cache → rebuild cache from existing mount (don't remount).

- [ ] **Step 5: Run tests — expect PASS**

---

### Task 3: NodePublishVolume — bind mount from staging

**Files:**
- Modify: `pkg/rclone/nodeserver.go`, `pkg/rclone/staging.go`
- Test: `pkg/rclone/nodeserver_stage_test.go`

**Interfaces:**
- Consumes: `getStagedVolume`, `rebuildOrRestage`
- Produces: staging-mode `NodePublishVolume` (bind only, no FUSE at target)

- [ ] **Step 1: Write failing test `TestNodePublishVolumeBindFromStaging`**

Uses fake mounter; asserts `Mount(stagingPath, targetPath, "", []string{"bind"})` called.

- [ ] **Step 2: Run test — expect FAIL**

- [ ] **Step 3: Refactor `NodePublishVolume`**

```go
func (ns *NodeServer) NodePublishVolume(ctx, req) {
    if !ns.Driver.staging {
        return ns.nodePublishVolumeDirect(ctx, req) // existing logic, renamed
    }
    return ns.nodePublishVolumeStaged(ctx, req)
}
```

`nodePublishVolumeStaged`:
1. Validate; lock `publish-{volumeID}-{targetPath}`
2. `stagingPath` — from `getStagedVolume` or derive from kubelet convention + `rebuildOrRestage` on cache miss
3. `prepareTargetDirectory(targetPath)` for bind target (directory, not FUSE)
4. `bindPublish(stagingPath, targetPath, readOnly)`
5. Return success (no FUSE at target, no mountContext at target)

- [ ] **Step 4: Implement self-heal on cache miss** (SeaweedFS Phase 1)

```go
func (ns *NodeServer) rebuildOrRestage(ctx, volumeID, stagingPath, params) error {
    if healthy, _ := IsMountPathHealthy(stagingPath, ns.mounter); healthy {
        return ns.rebuildStagedVolumeFromMount(ctx, volumeID, stagingPath, params)
    }
    if err := ns.forceCleanupMount(stagingPath); err != nil { ... }
    return ns.stageVolume(ctx, volumeID, stagingPath, params)
}
```

- [ ] **Step 5: Run tests — expect PASS**

---

### Task 4: NodeUnpublishVolume + NodeUnstageVolume

**Files:**
- Modify: `pkg/rclone/nodeserver_stage.go`, `pkg/rclone/nodeserver.go`, `pkg/rclone/staging.go`
- Test: `pkg/rclone/nodeserver_stage_test.go`

- [ ] **Step 1: Write failing tests**

- `TestNodeUnpublishVolumeUnbindOnly` — unmounts target, staging FUSE remains
- `TestNodeUnstageVolumeUnmountFUSE` — unmounts staging, deletes cache + MountState

- [ ] **Step 2: Run tests — expect FAIL**

- [ ] **Step 3: Implement**

`NodeUnpublishVolume` (staging mode):
- `unbindPublish(targetPath)` only
- Do NOT delete `stagedVolume` or FUSE

`NodeUnstageVolume`:
- Validate volumeID + stagingTargetPath
- Lock `stage-{volumeID}`
- `unmountVolume(mountCtx, stagingPath)` + `deleteStagedVolume`
- `DeleteState(volumeID, stagingPath)`

- [ ] **Step 4: Run tests — expect PASS**

---

### Task 5: MountState + boot remount for staging paths

**Files:**
- Modify: `pkg/rclone/mount_state.go`, `pkg/rclone/nodeserver.go`
- Test: `pkg/rclone/mount_state_test.go`, `pkg/rclone/nodeserver_stage_test.go`

- [ ] **Step 1: Add `StagingPath` field + secret key `stagingPath`**

```go
type MountState struct {
    VolumeID    string
    StagingPath string `json:"stagingPath"` // primary path when staging enabled
    TargetPath  string `json:"targetPath"` // legacy; same as StagingPath for staging mode
    // ...existing fields
}
```

- [ ] **Step 2: Update `SaveState` in `NodeStageVolume`** (not publish)

- [ ] **Step 3: Update `RemountState` to remount `StagingPath`**

- [ ] **Step 4: Migration in `deserializeSecret`**

If `stagingPath` empty but `targetPath` set → use `targetPath` (backward compat for existing secrets).

- [ ] **Step 5: Run tests — expect PASS**

---

### Task 6: CLI, Helm, docs

**Files:**
- Modify: `cmd/rcloneplugin/main.go`, `charts/values.yaml`, `charts/templates/csi-rclone-node.yaml`
- Modify: `docs/mount-recovery.md`
- Create: `hack/e2e/helm-values-staging.yaml`

- [ ] **Step 1: Add `--staging` flag (default `false`)**

- [ ] **Step 2: Helm `node.staging.enabled`** → env/arg to node DaemonSet

- [ ] **Step 3: Document upgrade path**

Enabling staging on existing cluster: rolling upgrade driver → recycle workload pods once (or let operator handle). New PVCs automatically use stage+publish.

- [ ] **Step 4: `helm-values-staging.yaml`**

```yaml
node:
  staging:
    enabled: true
  remount:
    enabled: true
```

---

### Task 7: Cluster validation TC-08

**Files:**
- Modify: `docs/integration-test-plan.md`
- Use: `hack/e2e/*`, `hack/e2e/cleanup.sh`

**TC-08: Staging + CSI restart without workload restart**

| Step | Action | Pass |
|------|--------|------|
| 1 | Deploy with `helm-values-staging.yaml` + operator (optional) | Stack Ready |
| 2 | Confirm staging path mounted: `ls /var/lib/kubelet/plugins/kubernetes.io/csi/.../globalmount` on node | FUSE present |
| 3 | Writer pod Running, `tail /data/e2e.log` | OK |
| 4 | Delete CSI node pod | New node pod Ready |
| 5 | Without rollout: `kubectl exec deploy/e2e-writer -- tail /data/e2e.log` | OK (no transport error) |
| 6 | `e2e-writer-propagated` (HostToContainer) also OK | OK |
| 7 | `cleanup.sh` | Clean |

- [ ] **Step 1: Build/push `linux/amd64` images to `ttl.sh`**

- [ ] **Step 2: Deploy + run TC-08**

- [ ] **Step 3: Record PASS/FAIL in `docs/integration-test-plan.md`**

- [ ] **Step 4: Cleanup cluster**

---

## Self-Review (plan vs spec)

| Requirement | Task | Status |
|-------------|------|--------|
| NodeStageVolume FUSE mount | Task 2 | Covered |
| NodePublishVolume bind mount | Task 3 | Covered |
| NodeUnstageVolume / NodeUnpublishVolume | Task 4 | Covered |
| Self-heal cache miss (SeaweedFS #212) | Task 3 Step 4 | Covered |
| Stale staging cleanup | Task 2 Step 4 | Covered |
| Phase B remount at staging | Task 5 | Covered |
| Backward compat flag | Task 1, 6 | Covered |
| Cluster E2E validation | Task 7 | Covered |
| Operator interaction documented | Task 6 | Covered (fallback) |

**Risks / open decisions:**

1. **TC-08 pass without `mountPropagation`** — may still FAIL for `e2e-writer` (no propagation). Plan tests both writers; success criteria: propagated writer MUST pass; non-propagated may still need operator (document honestly).

2. **RWX multi-pod** — staging enables single FUSE; bind mounts for multiple pods on same node. Ref-count in `publishRefs` optional for v1 (kubelet orders unstage after all unpublish).

3. **`NodeGetVolumeStats`** — currently checks `volumePath` (publish path). With staging, health should reflect staging FUSE health. Add Task 3 sub-step: if staging mode, resolve staging path from volumeID.

4. **Metrics collector** — `extractVolumeID(targetPath)` assumes publish path layout. Update to handle staging paths in Task 5 or 6.

---

## Execution Order

```
Task 1 → Task 2 → Task 3 → Task 4 → Task 5 → Task 6 → Task 7
```

Estimated: 7 tasks, cluster validation gates completion.

## Execution Handoff

**Plan saved to `docs/superpowers/plans/2026-07-12-nodestagevolume-staging.md`.**

**Two execution options:**

1. **Subagent-Driven (recommended)** — fresh subagent per task, review between tasks
2. **Inline Execution** — implement task-by-task in this session with checkpoints

**Which approach?**
