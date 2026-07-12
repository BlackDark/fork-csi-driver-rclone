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

## Volume recovery operator (Phase C)

The optional **volume recovery operator** is a node-local DaemonSet that complements driver self-healing (Phase A) and persisted remount (Phase B). It targets **application bind mounts** that remain stale inside workload pods after the CSI driver remounts the kubelet path.

### When to use it

- Workloads **without** `mountPropagation: HostToContainer` cannot see a remounted kubelet path until the pod is recreated.
- After CSI driver OOM/restart, app containers may still hold dead FUSE file descriptors even when the kubelet mount is healthy.

The operator periodically scans `/var/lib/kubelet/pods/*/volumes/kubernetes.io~csi/*/mount` on each node, reuses `IsMountPathHealthy` from the driver, and deletes affected workload pods so controllers recreate them.

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
