# Issue #58 Integration Test Plan

Validated on cluster context `informaten` (k3s v1.36.1, single node `debian-01`) on 2026-07-12.

Harness manifests: `hack/e2e/`

## Prerequisites

| Item | Notes |
|------|-------|
| Kubernetes | 1.20+, FUSE-capable nodes, privileged CSI node pods |
| S3 backend | Ephemeral MinIO in test namespace (or existing S3-compatible endpoint) |
| Images | Build `linux/amd64` images; push to cluster-pullable registry (used `ttl.sh` for ephemeral tests) |
| Tools | `kubectl`, `helm`, `docker` |
| Isolation | Dedicated namespace (e.g. `csi-rclone-e2e`) |

## Deploy stack

```bash
kubectl apply -f hack/e2e/namespace.yaml
kubectl apply -f hack/e2e/minio.yaml
helm upgrade --install csi-rclone-e2e ./charts \
  -n csi-rclone-e2e -f hack/e2e/helm-values.yaml --wait
kubectl apply -f hack/e2e/workloads.yaml
# optional operator
kubectl apply -f hack/e2e/operator.yaml
```

Helm values must set custom image + `node.remount.enabled=true` for Phase B tests.

## Test matrix

### TC-01 Baseline provision + mount (PASS)

**Goal:** PVC provisions, pod mounts, read/write works.

**Steps:**
1. Wait for `pvc/e2e-data` Bound
2. Wait for `deploy/e2e-writer` Ready
3. `kubectl exec deploy/e2e-writer -- tail /data/e2e.log`

**Pass criteria:**
- PVC Bound within 120s
- Pod Running
- Log file grows every 5s

**Result (2026-07-12):** PASS

---

### TC-02 Mount state persistence (PASS)

**Goal:** `--remount` persists state to node-scoped Secrets.

**Steps:**
1. After TC-01, `kubectl get secrets -n csi-rclone-e2e | grep rclone-mount-state`
2. Inspect label `csi.veloxpack.io/node=<nodeName>`

**Pass criteria:**
- One Secret per active volume on that node
- Secret contains `volumeId`, `targetPath`, `configData`

**Result:** PASS (3 secrets for 3 PVCs)

---

### TC-03 CSI driver restart + remount (PASS driver / FAIL workload without restart)

**Goal:** Node pod restart triggers `RemountAllStates`; kubelet paths recover.

**Steps:**
1. Record writer pod can read `/data/e2e.log`
2. `kubectl delete pod -l app=csi-rclone-node -n csi-rclone-e2e`
3. Wait for new node pod Ready
4. Check node logs for `Successfully remounted volume`
5. Without restarting workload: `kubectl exec deploy/e2e-writer -- tail /data/e2e.log`

**Pass criteria (driver):**
- Remount logs for all persisted volumes
- No driver crash loop

**Pass criteria (workload):**
- **Expected FAIL** without `mountPropagation` or pod restart: `Transport endpoint is not connected`

**Result:**
- Driver remount: **PASS**
- Workload without restart: **FAIL** (expected — confirms issue #58 bind-mount behavior)
- `mountPropagation: HostToContainer` alone: **FAIL** in this test (bind still stale after remount)

---

### TC-04 Workload recovery via rollout restart (PASS)

**Goal:** After TC-03, restarting workload re-binds to healthy kubelet mount.

**Steps:**
1. `kubectl rollout restart deploy/e2e-writer deploy/e2e-writer-propagated`
2. `kubectl exec deploy/e2e-writer -- date >> /data/recovered.log`

**Pass criteria:**
- New pods Running
- Write succeeds; prior `e2e.log` data retained (MinIO backend)

**Result:** PASS

---

### TC-05 Bad credentials error case (PASS)

**Goal:** Invalid S3 creds fail at I/O, not necessarily at pod schedule time.

**Steps:**
1. Apply `rclone-e2e-bad` StorageClass + `e2e-bad-creds` PVC + pod
2. `kubectl exec e2e-bad-creds-pod -- touch /data/should-fail`

**Pass criteria:**
- Pod may reach Running (CSI mount succeeds)
- Write returns I/O error

**Result:** PASS (`Input/output error`)

---

### TC-06 Volume recovery operator (PASS with CSI restart recovery)

**Goal:** Operator restarts workload pods after CSI node restart when kubelet paths are healthy but container bind mounts are stale.

**Steps:**
1. Deploy `hack/e2e/operator.yaml` (ensure `--csi-restart-recovery=true`, default)
2. Confirm workloads Running (TC-01)
3. `kubectl delete pod -l app=csi-rclone-node -n csi-rclone-e2e --grace-period=0`
4. Wait for new CSI node pod Ready
5. Watch operator logs for `CSI node pod restart detected` / `restarted pod after CSI node restart`
6. Confirm workload pods recreated with `volume.veloxpack.io/last-recovery` annotation
7. `kubectl exec deploy/e2e-writer -- tail /data/e2e.log` (no manual rollout)

**Pass criteria:**
- Operator restarts rclone workload pods on CSI node restart
- New pods can read/write without manual rollout
- Kubelet-path scanner still handles remount-disabled stale paths

**Result (2026-07-12):** PASS on `informaten`
- Operator logged `CSI node pod restart detected` + `restarted pod after CSI node restart` for `e2e-writer` and `e2e-writer-propagated` (~30s after CSI pod delete, 15s scan interval)
- Workload pod UIDs changed without manual `rollout restart`
- Post-recovery: `tail /data/e2e.log` OK, new file write OK (`post-operator-recovery.log`)
- No `Transport endpoint is not connected` after operator recovery

---

### TC-07 Remount disabled regression (FAIL without workload restart)

**Goal:** Document behavior when `--remount` is off.

**Steps:**
1. `helm upgrade ... --set node.remount.enabled=false`
2. Force-delete CSI node pod
3. Check workload + kubelet path health

**Pass criteria:**
- Documented expectation: kubelet paths stay stale; workloads break until manual pod restart + remount re-enabled

**Result:** Confirms Phase B is required for driver-side recovery

---

### TC-08 Staging + CSI restart without workload restart (FAIL after publish bind refresh)

**Goal:** Validate `NodeStageVolume` staging path plus `NodePublishVolume` bind mounts recover workload I/O after CSI node restart without restarting workload pods.

**Deploy:**
```bash
docker buildx build --platform linux/amd64 \
  -t ttl.sh/csi-rclone-staging-20260712114245:8h --push .
kubectl apply -f hack/e2e/namespace.yaml
kubectl apply -f hack/e2e/minio.yaml
helm upgrade --install csi-rclone-e2e ./charts -n csi-rclone-e2e \
  -f hack/e2e/helm-values.yaml \
  -f hack/e2e/helm-values-staging.yaml \
  --set image.rclone.repository=ttl.sh/csi-rclone-staging-20260712114245 \
  --set image.rclone.tag=8h --wait --timeout 120s
kubectl apply -f hack/e2e/workloads.yaml
```

`hack/e2e/helm-values-staging.yaml` only enables staging/remount; the base e2e values were also required to create `storageclass/rclone-e2e`.

**Result (2026-07-12):** FAIL on `informaten` after publish-bind refresh

| Check | Result | Evidence |
|-------|--------|----------|
| CSI image | PASS | Built/pushed `linux/amd64` image `ttl.sh/csi-rclone-staging-20260712114245:8h` |
| Stack ready | PASS | `e2e-writer` and `e2e-writer-propagated` Running on `debian-01`; PVCs Bound |
| Staging calls | PASS | CSI node logs showed `NodeStageVolume`, `Successfully staged volume`, and bind publish from `.../globalmount` |
| Baseline `e2e-writer` tail | PASS | `kubectl exec deploy/e2e-writer -- tail /data/e2e.log` returned `Sun Jul 12 09:45:13 UTC 2026` |
| Baseline `e2e-writer-propagated` tail | PASS | `kubectl exec deploy/e2e-writer-propagated -- tail /data/e2e.log` returned `Sun Jul 12 09:45:13 UTC 2026` |
| CSI node restart | PASS | Force-deleted `csi-rclone-node-x2sj2`; new `csi-rclone-node-qrf99` Ready; workload pod UIDs unchanged |
| Publish bind refresh | PASS | CSI node logs showed `Refreshed publish bind` for both workload publish paths after remount |
| Post-restart `e2e-writer` tail | FAIL | `Transport endpoint is not connected` after driver-side publish bind refresh |
| Post-restart `e2e-writer-propagated` tail | FAIL | `Transport endpoint is not connected` after driver-side publish bind refresh |

**Notes:**
- New CSI node logs reported `Remounting 3 persisted mount states`, `Successfully remounted volume`, and `Refreshed publish bind` for the writer publish paths.
- Driver-side refresh repaired kubelet publish mount paths, but existing container bind mounts still referenced the old stale mount object.
- TC-08 success criterion was not met before Phase D: both writers failed after CSI restart.

---

### TC-09 Container remount + hostPID gate (pending)

**Goal:** Validate Phase D container remount and `hostPID` requirement.

**Steps:**
1. Deploy with `node.hostPID: false` (override auto default) → CSI restart → expect ENOTCONN
2. Upgrade to `node.hostPID: true` → CSI restart → expect I/O OK without workload restart
3. CSI node logs: `Remounted container mount` lines; `/proc` in CSI pod >> 100 entries

**Pass criteria:** hostPID gate demonstrated; recovery after step 2 for both `e2e-writer` and `e2e-writer-propagated`.

---

### TC-08 Staging + CSI restart (re-run after Phase D — pending)

Re-run TC-08 with `hack/e2e/helm-values-staging.yaml` (`hostPID: true`). Expect PASS for both writers without workload restart.

---

## Cleanup

Fast teardown (no long `--wait`; handles stuck Terminating PVCs/PVs and orphan namespaces):

```bash
./hack/e2e/cleanup.sh
```

Manual equivalent:

```bash
helm uninstall csi-rclone-e2e -n csi-rclone-e2e
kubectl delete clusterrole,clusterrolebinding volume-recovery-operator-e2e --wait=false
kubectl delete sc rclone-e2e-bad rclone-e2e --wait=false
kubectl delete all,pvc --all -n csi-rclone-e2e --grace-period=0 --force --wait=false
# if namespace stuck Terminating: patch PV/PVC finalizers, then:
kubectl get namespace csi-rclone-e2e -o json | jq '.spec.finalizers=[]' | \
  kubectl replace --raw /api/v1/namespaces/csi-rclone-e2e/finalize -f -
# if namespace object gone but PVCs remain: recreate ns, patch PVC finalizers, delete again
```

## Findings summary

| Component | Works | Limitation |
|-----------|-------|------------|
| Phase A publish healing | Yes | Only when kubelet calls `NodePublishVolume` |
| Phase B boot remount | Yes | Recovers kubelet paths after CSI pod death |
| App bind mounts | No auto-heal | Need pod restart or future `NodeStageVolume` architecture |
| Phase C operator | Yes (CSI restart path) | Also scans kubelet-path corruption when remount disabled |
| Bad creds | Clear I/O failure | Pod may still start |

## Future automated CI suggestions

1. **Kind/k3d job** with MinIO + helm install from built image
2. **Scripted TC-01..TC-05** as bash with `kubectl wait` + exit codes
3. **TC-03/TC-04** as single script: restart CSI node → assert remount logs → assert workload fails → rollout restart → assert recovery
4. **Operator tests** behind `go test -tags=integration` (already scaffolded)
5. **NodeStageVolume** tests deferred until staging architecture is implemented

## NodeStageVolume note

`NodeStageVolume` + staging path was **not** implemented and was **not** tested. It remains the CSI-native path to self-healing without workload restarts (SeaweedFS #212 pattern).
