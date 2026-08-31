#!/usr/bin/env bash
# Teardown for csi-rclone e2e. Prefer deleting workloads/PVCs while CSI is still
# installed; uninstalling helm first leaves Bound PVCs that stick Terminating.
# See docs/cluster-e2e.md.
set -euo pipefail

NS="${NS:-csi-rclone-e2e}"
RELEASE="${RELEASE:-csi-rclone-e2e}"
DRIVER_NS="${DRIVER_NS:-$NS}"

echo "==> delete workloads first (while CSI can unmount)"
kubectl delete deploy,sts,ds,job,pod --all -n "$NS" --grace-period=0 --wait=false 2>/dev/null || true
# Give kubelet/CSI a moment when driver is still present
sleep 2

echo "==> delete PVCs in app ns"
kubectl delete pvc --all -n "$NS" --grace-period=0 --wait=false 2>/dev/null || true

echo "==> uninstall helm release (no wait)"
helm uninstall "$RELEASE" -n "$DRIVER_NS" 2>/dev/null || true

echo "==> delete cluster-scoped e2e resources"
kubectl delete clusterrole,clusterrolebinding volume-recovery-operator-e2e volume-recovery-operator \
  --ignore-not-found --wait=false 2>/dev/null || true
kubectl delete sc rclone-e2e-bad rclone-e2e rclone-v060 rclone-v060-bad \
  --ignore-not-found --wait=false 2>/dev/null || true

echo "==> force-delete remaining namespaced objects"
kubectl delete all --all -n "$NS" --grace-period=0 --force --wait=false 2>/dev/null || true
kubectl delete pvc --all -n "$NS" --grace-period=0 --wait=false 2>/dev/null || true
kubectl delete secrets -n "$NS" -l csi.veloxpack.io/mount-state \
  --grace-period=0 --wait=false 2>/dev/null || true
if [[ "$DRIVER_NS" != "$NS" ]]; then
  kubectl delete all --all -n "$DRIVER_NS" --grace-period=0 --force --wait=false 2>/dev/null || true
  kubectl delete secrets -n "$DRIVER_NS" -l csi.veloxpack.io/mount-state \
    --grace-period=0 --wait=false 2>/dev/null || true
fi

echo "==> clear PV finalizers for e2e claims (test ns only)"
clear_ns_pvs() {
  local ns="$1"
  while IFS= read -r pv; do
    [[ -z "$pv" ]] && continue
    kubectl patch pv "$pv" -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
    kubectl delete pv "$pv" --grace-period=0 --wait=false 2>/dev/null || true
  done < <(kubectl get pv -o json 2>/dev/null | jq -r \
    --arg ns "$ns" '.items[] | select(.spec.claimRef.namespace==$ns) | .metadata.name')
}
clear_ns_pvs "$NS"
[[ "$DRIVER_NS" != "$NS" ]] && clear_ns_pvs "$DRIVER_NS"

# Orphan namespace: ns object gone but PVCs/Pods remain Terminating.
for orphan_ns in "$NS" "$DRIVER_NS"; do
  if ! kubectl get namespace "$orphan_ns" >/dev/null 2>&1; then
    if kubectl get pvc -A -o json 2>/dev/null | jq -e --arg ns "$orphan_ns" \
      '.items[] | select(.metadata.namespace==$ns)' >/dev/null; then
      echo "==> recreate namespace $orphan_ns to clear orphan PVC finalizers"
      kubectl create namespace "$orphan_ns" 2>/dev/null || true
      while IFS= read -r pvc; do
        [[ -z "$pvc" ]] && continue
        kubectl patch pvc "$pvc" -n "$orphan_ns" -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
      done < <(kubectl get pvc -n "$orphan_ns" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
      kubectl delete pvc --all -n "$orphan_ns" --grace-period=0 --wait=false 2>/dev/null || true
    fi
  fi
done

echo "==> delete namespace(s) (no wait)"
kubectl delete namespace "$NS" --grace-period=0 --wait=false 2>/dev/null || true
[[ "$DRIVER_NS" != "$NS" ]] && kubectl delete namespace "$DRIVER_NS" --grace-period=0 --wait=false 2>/dev/null || true

finalize_ns() {
  local ns="$1"
  if kubectl get namespace "$ns" -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Terminating; then
    echo "==> finalize stuck namespace $ns"
    kubectl get namespace "$ns" -o json | jq '.spec.finalizers=[]' | \
      kubectl replace --raw "/api/v1/namespaces/${ns}/finalize" -f - >/dev/null
  fi
}
finalize_ns "$NS"
[[ "$DRIVER_NS" != "$NS" ]] && finalize_ns "$DRIVER_NS"

echo "==> verify"
if kubectl get pvc,pv,sc,pods -A 2>/dev/null | rg -q "csi-rclone-e2e|rclone-e2e|csi-rclone-v060|rclone-v060"; then
  echo "WARN: some e2e resources may remain — inspect manually (see docs/cluster-e2e.md)"
  exit 1
fi
echo "cleanup complete"
