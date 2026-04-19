#!/usr/bin/env bash
set -euo pipefail

if kubectl get deployment metrics-server -n kube-system >/dev/null 2>&1; then
  echo "metrics-server already present in kube-system; skipping install."
else
  echo "metrics-server not found; installing latest official components..."
  kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
fi
