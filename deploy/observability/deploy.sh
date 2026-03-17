#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
kubectl apply -f "$DIR/tempo.yaml"
kubectl apply -f "$DIR/pyroscope.yaml"
kubectl apply -f "$DIR/grafana.yaml"
echo "Waiting for observability stack..."
kubectl -n novastor-system rollout status deployment/tempo --timeout=60s
kubectl -n novastor-system rollout status deployment/pyroscope --timeout=60s
kubectl -n novastor-system rollout status deployment/grafana --timeout=120s
echo "Grafana: http://$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}'):30300"
