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

- Prefer `vfs-cache-mode=writes` or `minimal` over `full` unless read caching is required
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

This is the CSI-native self-heal path for stale FUSE mounts. With workload `mountPropagation: HostToContainer`, refreshed staging mounts can propagate into running containers without a workload rollout. Workloads without mount propagation may still need a pod restart; the volume recovery operator remains a fallback for those pods.

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

The operator periodically scans `/var/lib/kubelet/pods/*/volumes/kubernetes.io~csi/*/mount` on each node, reuses `IsMountPathCorrupted` from the driver, and deletes affected workload pods so controllers recreate them.

When Phase B remount keeps kubelet paths healthy, the operator also watches for **CSI node pod restarts** on the same node and restarts workload pods that use rclone volumes. Container bind mounts cannot be refreshed in-place without `mountPropagation` or a pod restart.

### Safety rails

- Skips `kube-system`, pods named `*csi-rclone*`, and pods owned by the CSI DaemonSet
- Rate limit: one recovery delete per pod per hour via `volume.veloxpack.io/last-recovery`
- Only volumes backed by `rclone.csi.veloxpack.io` are acted on

### Deploy

See [deploy/volume-recovery-operator/README.md](../deploy/volume-recovery-operator/README.md).

```bash
kubectl apply -k deploy/volume-recovery-operator/
```

### References

- [IBM Spectrum Scale CSI recovery](https://www.ibm.com/docs/en/spectrum-scale-csi)
- [JuiceFS mount recovery patterns](https://juicefs.com/docs/csi/introduction/)

## Related components

- **Phase B:** Persisted remount state in Kubernetes Secrets for boot-time recovery.
