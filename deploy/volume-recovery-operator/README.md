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
| `--scan-interval` | `60s` | Scan loop interval |
| `--csi-node-label` | `app=csi-rclone-node` | Label selector for CSI node pods |
| `--csi-restart-recovery` | `true` | Restart rclone workload pods after CSI node restart |

`NODE_NAME` is injected from the downward API (`spec.nodeName`).

## Behavior

1. Watch local CSI node pod (`app=csi-rclone-node`); on restart, restart workload pods using rclone volumes on this node
2. Walk `/var/lib/kubelet/pods/*/volumes/kubernetes.io~csi/*/mount`
3. Run the same health probe as the CSI driver (`IsMountPathCorrupted`)
4. Match mount pod UID to pods scheduled on this node
5. Skip `kube-system`, pods named `*csi-rclone*`, and CSI DaemonSet pods
6. Rate limit: at most one recovery delete per pod per hour (`volume.veloxpack.io/last-recovery` annotation)
7. Delete the pod (grace period 0) so controllers recreate it with fresh mounts

## Uninstall

```bash
kubectl delete -k deploy/volume-recovery-operator/
```

## Image build note

The default driver image builds only `rcloneplugin`. To run this operator, add a build target for `cmd/volume-recovery-operator` and copy the binary to `/volume-recovery-operator`, or build a dedicated image.
