#!/usr/bin/env bash
# Fast teardown for csi-rclone e2e namespace. Avoids long --wait timeouts.
set -euo pipefail

NS="${NS:-csi-rclone-e2e}"
RELEASE="${RELEASE:-csi-rclone-e2e}"

echo "==> uninstall helm release (no wait)"
helm uninstall "$RELEASE" -n "$NS" 2>/dev/null || true

echo "==> delete cluster-scoped e2e resources"
kubectl delete clusterrole,clusterrolebinding volume-recovery-operator-e2e volume-recovery-operator \
  --ignore-not-found --wait=false 2>/dev/null || true
kubectl delete sc rclone-e2e-bad rclone-e2e --ignore-not-found --wait=false 2>/dev/null || true

echo "==> force-delete namespaced workloads"
kubectl delete all --all -n "$NS" --grace-period=0 --force --wait=false 2>/dev/null || true
kubectl delete pvc --all -n "$NS" --grace-period=0 --wait=false 2>/dev/null || true
kubectl delete secrets -n "$NS" -l csi.veloxpack.io/mount-state \
  --grace-period=0 --wait=false 2>/dev/null || true

echo "==> clear PV finalizers for e2e claims"
while IFS= read -r pv; do
  [[ -z "$pv" ]] && continue
  kubectl patch pv "$pv" -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
  kubectl delete pv "$pv" --grace-period=0 --wait=false 2>/dev/null || true
done < <(kubectl get pv -o json 2>/dev/null | jq -r \
  --arg ns "$NS" '.items[] | select(.spec.claimRef.namespace==$ns) | .metadata.name')

# Orphan namespace: ns object gone but PVCs/Pods remain Terminating.
if ! kubectl get namespace "$NS" >/dev/null 2>&1; then
  if kubectl get pvc -A -o json 2>/dev/null | jq -e --arg ns "$NS" \
    '.items[] | select(.metadata.namespace==$ns)' >/dev/null; then
    echo "==> recreate namespace to clear orphan PVC finalizers"
    kubectl create namespace "$NS" 2>/dev/null || true
    while IFS= read -r pvc; do
      [[ -z "$pvc" ]] && continue
      kubectl patch pvc "$pvc" -n "$NS" -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
    done < <(kubectl get pvc -n "$NS" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
    kubectl delete pvc --all -n "$NS" --grace-period=0 --wait=false 2>/dev/null || true
  fi
fi

echo "==> delete namespace (no wait)"
kubectl delete namespace "$NS" --grace-period=0 --wait=false 2>/dev/null || true

if kubectl get namespace "$NS" -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Terminating; then
  echo "==> finalize stuck namespace"
  kubectl get namespace "$NS" -o json | jq '.spec.finalizers=[]' | \
    kubectl replace --raw "/api/v1/namespaces/${NS}/finalize" -f - >/dev/null
fi

echo "==> verify"
if kubectl get pvc,pv,sc,pods -A 2>/dev/null | rg -q "csi-rclone-e2e|rclone-e2e"; then
  echo "WARN: some e2e resources may remain — inspect manually"
  exit 1
fi
echo "cleanup complete"
