# Cluster e2e testing & cleanup

Guide for manual / ttl.sh cluster validation of the CSI driver and volume-recovery-operator (arm64/amd64 as needed).

Related: [integration-test-plan.md](./integration-test-plan.md), [mount-recovery.md](./mount-recovery.md), `hack/e2e/`.

## Deploy (ttl example)

1. Build/push arch-matched images (e.g. `linux/arm64` → `ttl.sh/…:24h`).
2. Apply backend + namespaces (`hack/e2e/` or equivalent rustfs/minio fixtures).
3. `helm upgrade --install … ./charts -f values` with `volumeRecoveryOperator.enabled=true`.
4. Apply writer workloads; smoke append to the PVC.
5. Prefer `volumeRecoveryOperator.logLevel: 2` (default).

## Operator checks worth asserting

| Check | How |
|-------|-----|
| No API throttle on foreign PVCs | Operator logs: no `client-side throttling` for unrelated ns (media/monitoring/…) |
| Ownership filter | Stale/restart actions only for this provisioner / `fuse.rclone` |
| Recovery Events | `kubectl get events -n <app> --field-selector reason=StaleCSIMount` or `CSINodeUIDChanged` — involved object is the **controller owner** (ReplicaSet/…), not the replacement Pod |
| Orphan FUSE | Inject dead UID + `SIGSTOP` rclone; delete CSI node; expect lazy-umount + abort/kill; CSI Ready without manual umount |

## Cleanup order (important)

Helm uninstall **before** deleting app workloads leaves PVCs Bound while the CSI driver is gone. Kubelet cannot finish volume teardown → pods Error/Terminating → namespace stuck Terminating.

**Correct order:**

1. Scale/delete **workloads** (Deployments/Pods) while CSI node is still Ready.
2. Delete PVCs; wait until related PVs are gone (or Released + deleted).
3. `helm uninstall <release> -n <driver-ns>`
4. Delete app + driver namespaces.
5. Delete **test** StorageClasses only (e.g. `rclone-e2e*`, `rclone-v060*`).
6. If an orphan inject was used (`deadbeef-…` path): host `umount -l`, kill leftover rclone, `rm -rf` that pod UID dir under kubelet.
7. **Never** strip finalizers or delete PVs for non-test volumes (media, monitoring, …).

Helper: `NS=csi-rclone-e2e RELEASE=csi-rclone-e2e ./hack/e2e/cleanup.sh` (deletes workloads/PVCs before relying on helm teardown; still prefer step 1–2 while CSI is up).

## If already stuck (CSI gone, NS Terminating)

**Test resources only:**

```bash
NS=csi-rclone-v060-app   # your app test ns

kubectl -n "$NS" delete pod --all --force --grace-period=0

for pvc in $(kubectl -n "$NS" get pvc -o name); do
  kubectl -n "$NS" patch "$pvc" --type=merge -p '{"metadata":{"finalizers":null}}'
done
kubectl -n "$NS" delete pvc --all --force --grace-period=0

# Only PVs that belonged to those test PVCs:
for pv in $(kubectl get pv -o json | jq -r --arg ns "$NS" \
  '.items[] | select(.spec.claimRef.namespace==$ns) | .metadata.name'); do
  kubectl patch pv "$pv" --type=merge -p '{"metadata":{"finalizers":null}}'
  kubectl delete pv "$pv" --force --grace-period=0
done

kubectl delete ns "$NS" --wait=false
```

Clear any host `fuse.rclone` mounts under deleted UIDs if they remain.
