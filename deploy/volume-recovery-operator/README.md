# Volume Recovery Operator

Optional node-local DaemonSet that detects stale rclone CSI bind mounts under the kubelet pod volumes directory and restarts affected **workload** pods.

## Prerequisites

- rclone CSI driver Phase A mount health checks deployed
- Workloads should use `mountPropagation: HostToContainer` for in-container recovery without restart when possible

## Install

Build or use an image that includes the `/volume-recovery-operator` binary, then apply:

```bash
kubectl apply -k deploy/volume-recovery-operator/
```

The manifest expects the `system` namespace (same as the CSI driver base install).

## Configuration

DaemonSet args (see `daemonset.yaml`):

| Flag | Default | Description |
|------|---------|-------------|
| `--kubelet-dir` | `/var/lib/kubelet` | Kubelet state root |
| `--provisioner` | `rclone.csi.veloxpack.io` | CSI driver to recover |
| `--scan-interval` | `60s` | Interval for CSI-UID and stale-mount loops |
| `--csi-node-label` | `app=csi-rclone-node` | Label selector for CSI node pods |
| `--csi-restart-recovery` | `true` | Restart rclone workload pods after CSI node restart |
| `--csi-restart-ready-timeout` | `90s` | Max wait for CSI node Ready after restart |
| `--mount-probe-timeout` | `3s` | Bound mount health probe; timeout treated as corrupted |
| `--orphan-lazy-umount` | `true` | Lazy-umount publish paths whose pod UID is gone |
| `--orphan-fuse-abort` | `true` | After orphan lazy-umount, abort FUSE conn via `/sys/fs/fuse/connections/<id>/abort` |
| `--orphan-kill-mount-process` | `true` | After orphan lazy-umount, best-effort kill hung mount servers (needs `hostPID`) |

`NODE_NAME` is injected from the downward API (`spec.nodeName`).

The DaemonSet uses `hostPID: true` so kill can see host/CSI rclone PIDs. It mounts host `/sys/fs/fuse` RW for abort writes. The kubelet-pods hostPath is mounted `readOnly: false` with `mountPropagation: Bidirectional` (requires privileged). `HostToContainer` only propagates mounts into the container; umounts do not clear the host. Bidirectional is required so orphan lazy-umount removes host `fuse.rclone` entries.

Abort and kill run **only** for orphan publish paths whose pod UID is absent from the node API — never for live CSI mounts or staging `globalmount`.

## Behavior

CSI-UID recovery and stale-mount scanning run on **separate goroutines/tickers** so a hung FUSE probe cannot starve restart detection.

1. Watch local CSI node pod (`app=csi-rclone-node`); on restart, wait Ready, then restart workload pods using rclone volumes on this node
2. Walk `/var/lib/kubelet/pods/*/volumes/kubernetes.io~csi/*/mount`, filter to this `--provisioner` via kubelet `vol_data.json` (or `fuse.rclone` fallback when `vol_data.json` is missing), then probe (timeout → corrupted; never descend into FUSE trees)
3. Match mount pod UID to pods scheduled on this node
4. If pod UID is gone: resolve FUSE conn id from mountinfo → lazy-umount (`MNT_DETACH`) → abort FUSE conn → best-effort kill mount process
5. Skip `kube-system`, pods named `*csi-rclone*`, and CSI DaemonSet pods
6. Rate limit: at most one recovery delete per workload per hour (in-memory + `volume.veloxpack.io/last-recovery` on the controller owner)
7. Emit a Warning Event on the controller owner; annotate the owner with `volume.veloxpack.io/last-recovery`; delete the Pod (no annotate on the dying Pod). Best-effort: annotate the replacement Pod once recreated (async, ~30s).
8. If delete hits `NotFound`, treat as done — do not Error-log.

Ownership checks prefer node-local `vol_data.json` so foreign CSI volumes do not generate API GETs or mount probes.
## Uninstall

```bash
kubectl delete -k deploy/volume-recovery-operator/
```

## Image build note

The default driver image builds only `rcloneplugin`. To run this operator, add a build target for `cmd/volume-recovery-operator` and copy the binary to `/volume-recovery-operator`, or build a dedicated image.
