# Mount Recovery

This document covers OOM mitigation, mount propagation, and recovery behavior for the rclone CSI driver.

## OOM and memory tuning

The CSI node pod runs rclone as a FUSE mount daemon. Memory use grows with VFS cache settings, concurrent transfers, and the number of mounted volumes.

### Container memory vs VFS cache

Set the node container memory limit high enough to cover:

- rclone process baseline (~50–100 MiB per mount)
- VFS cache (`vfs-cache-max-size`) when cache mode is not `off`
- Buffer for upload/download spikes

Rule of thumb: **container memory limit ≥ 2× `vfs-cache-max-size` + 150 MiB headroom** per active mount. Example: `vfs-cache-max-size=512M` → set `node.resources.rclone.limits.memory` to at least `1Gi`.

If the driver OOMs, FUSE mounts can become stale (kernel mount entry remains but userspace is gone). Phase A publish-path recovery detects and remounts stale CSI mount points; see [Issue #58](https://github.com/veloxpack/csi-driver-rclone/issues/58).

### Reduce OOM risk

- Prefer `vfs-cache-mode=writes` over `full` unless read caching is required (`off`/`minimal` use less memory but cannot append — see [driver-parameters.md](driver-parameters.md))
- Cap `vfs-cache-max-size` to a fraction of container memory
- Use a hostPath or emptyDir for `cache_dir` / `temp_dir` to keep page cache pressure out of the container cgroup
- Increase `node.resources.rclone.limits.memory` before raising cache sizes

## mountPropagation for application pods

CSI mounts the volume at the kubelet path (`/var/lib/kubelet/pods/.../mount`). Application containers only see the volume if it is propagated into the pod mount namespace.

For workloads that must survive CSI driver restarts without pod restart, set on the **workload** pod spec:

```yaml
securityContext:
  # ...
volumeMounts:
  - name: data
    mountPath: /data
    mountPropagation: HostToContainer
```

`HostToContainer` propagates host (kubelet) mounts into the container. Without propagation, a remounted CSI path may not appear inside the app container until the pod is recreated.

**Note:** Zero-downtime recovery inside running app containers is not possible without `mountPropagation: HostToContainer` (or `Bidirectional` where appropriate).

## Driver self-healing (Phase A)

On `NodePublishVolume`:

1. If the target is already mounted and `ReadDir` succeeds, publish is idempotent when the driver still holds mount context.
2. After driver restart (no mount context), a healthy but driverless FUSE mount is force-unmounted and remounted.
3. Corrupted mounts (`ENOTCONN` / transport errors) are force-unmounted before remount.

## Staging architecture

Staging mode enables the CSI `NodeStageVolume` + `NodePublishVolume` flow. The driver mounts one rclone FUSE filesystem per volume per node at kubelet's node-global staging path, then bind-mounts that staging path into each workload pod target.

Enable it with:

```yaml
node:
  staging:
    enabled: true
  remount:
    enabled: true
```

In this mode, `NodeStageVolume` owns the FUSE mount and persists the staging path for boot-time remount. `NodePublishVolume` only creates a bind mount from staging to the pod path. After a CSI node pod restart, persisted remount restores the staging FUSE mount; subsequent publish calls can rebuild in-memory staging state from the healthy staging mount or restage if the mount is stale.

Staging improves the CSI stage/publish split and kubelet-path recovery, but it does **not** yet eliminate the need for the volume recovery operator or a workload pod restart after a CSI node pod restart (integration TC-08 currently fails). Running application containers can still hold stale bind mounts or dead FUSE file descriptors until the pod is recreated.

With workload `mountPropagation: HostToContainer`, refreshed kubelet mounts may propagate into running containers in some cases, but that is not sufficient for zero-downtime recovery today. Workloads without mount propagation still need a pod restart; the volume recovery operator remains required for automated recovery after CSI restarts.

### Upgrade path

Staging is disabled by default for rollback safety. To enable it on an existing cluster:

1. Upgrade the driver with `node.staging.enabled=true` and `node.remount.enabled=true`.
2. Let the CSI node DaemonSet roll.
3. Recycle workload pods once so kubelet stages existing volumes through the new stage+publish path, or let the volume recovery operator restart affected pods.

New PVC mounts automatically use stage+publish after staging is enabled.

## Volume recovery operator (Phase C)

The optional **volume recovery operator** is a node-local DaemonSet that complements driver self-healing (Phase A) and persisted remount (Phase B). It targets **application bind mounts** that remain stale inside workload pods after the CSI driver remounts the kubelet path.

### When to use it

- Workloads **without** `mountPropagation: HostToContainer` cannot see a remounted kubelet path until the pod is recreated.
- After CSI driver OOM/restart, app containers may still hold dead FUSE file descriptors even when the kubelet mount is healthy.

The operator periodically scans CSI publish paths on each node. It retains only volumes for this
driver (kubelet `vol_data.json`, with a `fuse.rclone` fallback), probes mount health with a bounded
three-second timeout, then deletes affected workload pods so controllers recreate them. It records a
Warning Event on the controller owner before deletion. After a successful delete it sets
`volume.veloxpack.io/last-recovery` on that owner and, best-effort, on the replacement Pod.
It does not annotate the dying Pod. Cooldown also uses in-memory owner keys. Concurrent deletes that
hit `NotFound` are treated as already done. CSI-UID restart recovery and stale-mount scans run on
independent loops so an orphan or hung FUSE probe cannot starve restart detection.

At most one timed probe can wait on uninterruptible FUSE I/O. While it is occupied, later mounts
are skipped until the next scan instead of being treated as corrupted.

When a stale publish path's pod UID is gone, the operator can lazy-umount it (`MNT_DETACH`, default
on), then abort that FUSE connection only when it has no remaining mount and best-effort kill a hung
mount server so CSI node teardown is not wedged by residual userspace FUSE. The operator mounts
kubelet-pods with `mountPropagation: Bidirectional` so lazy-umount clears the host mount table,
uses `hostPID: true` for kill visibility, and mounts host `/sys/fs/fuse` RW for abort.

When Phase B remount + skip-bind-refresh keeps kubelet/staging healthy while container binds stay stale, the operator is **required** for automated recovery. It watches for **CSI node pod restarts** on the same node, waits until the new CSI node pod is Ready (default 90s, then proceeds), and restarts workload pods that use rclone volumes. Container bind mounts cannot be refreshed in-place without `mountPropagation` or a pod restart.

### Safety rails

- Skips `kube-system`, pods named `*csi-rclone*`, and pods owned by the CSI DaemonSet
- Rate limit: one recovery delete per workload (~controller owner) per hour via in-memory key + `volume.veloxpack.io/last-recovery` on the owner (and best-effort on the replacement Pod)
- Only volumes backed by `rclone.csi.veloxpack.io` are acted on
- Mount probe timeout avoids permanent wedging on zombie `fuse.rclone` under deleted pod UIDs
- Orphan lazy-umount only when the pod UID is absent from the node (never staging `globalmount`)
- Orphan FUSE abort / mount-process kill only after UID-gone orphan lazy-umount (never live CSI mounts)

### Deploy

Helm (preferred):

```yaml
volumeRecoveryOperator:
  enabled: true
  csiRestartRecovery: true   # default
  # csiNodeLabel: ""         # empty → app=<node.name>
  # csiRestartReadyTimeout: 90s
  # mountProbeTimeout: 3s
  # orphanLazyUmount: true
  # orphanFuseAbort: true
  # orphanKillMountProcess: true
```

Or apply the kustomize manifests:

```bash
kubectl apply -k deploy/volume-recovery-operator/
```

### Optional: Reloader instead of the operator

If you already run [stakater/Reloader](https://github.com/stakater/Reloader) and prefer rolling Deployments/StatefulSets over force-deleting pods, you can approximate the same **pod recreate → remount** outcome without the volume recovery operator.

**Prefer the volume recovery operator.** Reloader does not scan mount health, restarts all annotated replicas (all nodes), requires same-namespace ConfigMap `data` changes, and silently does nothing if annotations or Reloader are missing. Do **not** run Reloader recovery signaling and the operator together — double restarts.

Per app namespace:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: rclone-csi-recovery
  namespace: media
data:
  epoch: "0"
```

Annotate each rclone CSI consumer Deployment/StatefulSet:

```yaml
metadata:
  annotations:
    configmap.reloader.stakater.com/reload: "rclone-csi-recovery"
```

After CSI remount (or when workloads hit `Transport endpoint is not connected`), bump ConfigMap **data** (annotation-only bumps are ignored by Reloader):

```bash
kubectl -n media patch configmap rclone-csi-recovery \
  --type merge -p "{\"data\":{\"epoch\":\"$(date -u +%Y%m%dT%H%M%SZ)\"}}"
```

### References

- [IBM Spectrum Scale CSI recovery](https://www.ibm.com/docs/en/spectrum-scale-csi)
- [JuiceFS mount recovery patterns](https://juicefs.com/docs/csi/introduction/)

## Related components

- **Phase B:** Persisted remount state in Kubernetes Secrets for boot-time recovery.
