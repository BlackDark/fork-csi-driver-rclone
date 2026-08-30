#!/usr/bin/env bash
# Probe cluster nodes and print helm --set args for controller placement.
# Prefer chart Exists affinity (USE_CHART_CONTROL_PLANE=1, default after chart fix).
set -euo pipefail

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl required" >&2
  exit 1
fi

nodes_json=$(kubectl get nodes -o json)
node_count=$(jq '.items | length' <<<"$nodes_json")
cp_nodes=$(jq '[.items[] | select(.metadata.labels["node-role.kubernetes.io/control-plane"] != null)] | length' <<<"$nodes_json")
workers=$(jq '[.items[] | select(
  .metadata.labels["node-role.kubernetes.io/control-plane"] == null
  and .metadata.labels["node-role.kubernetes.io/master"] == null
)] | length' <<<"$nodes_json")

cp_val=$(jq -r '
  [.items[].metadata.labels["node-role.kubernetes.io/control-plane"] // empty]
  | .[0] // empty
' <<<"$nodes_json")

echo "# nodes=${node_count} control-plane=${cp_nodes} workers=${workers} cp_label_value=${cp_val:-<empty>}" >&2

# Prefer chart Exists affinity once fixed (default).
if [[ "${USE_CHART_CONTROL_PLANE:-1}" == "1" ]]; then
  if (( cp_nodes > 0 && workers == 0 )) || [[ "${FORCE_CONTROL_PLANE:-0}" == "1" ]]; then
    echo '--set controller.runOnControlPlane=true'
  else
    echo '--set controller.runOnControlPlane=false'
  fi
  exit 0
fi

# Pre-fix workaround: equality nodeSelector by detected label value.
if (( cp_nodes == 0 )); then
  echo '--set controller.runOnControlPlane=false'
elif (( workers == 0 )); then
  if [[ -z "$cp_val" ]]; then
    # kubeadm-style empty value — emit values snippet hint; --set-string of "" is awkward
    echo 'WRITE_VALUES:controller.nodeSelector.node-role.kubernetes.io/control-plane: ""' >&2
    echo '--set controller.runOnControlPlane=false'
  else
    echo '--set controller.runOnControlPlane=false'
    echo "--set-string controller.nodeSelector.node-role\\.kubernetes\\.io/control-plane=${cp_val}"
  fi
else
  echo '--set controller.runOnControlPlane=false'
fi
