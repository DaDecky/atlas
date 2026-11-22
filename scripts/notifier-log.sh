#!/usr/bin/env bash
# Appends the notifier log to FILE across pod restarts and reschedules, for scripts/error-budget.py.
# kubectl logs reads only the current pod, so start this before the game day and stop it afterwards.
# Each reconnect rereads the last hour, so lines repeat. error-budget.py keeps the first delivery of each hazard.
set -u

out=${1:?usage: notifier-log.sh FILE}
namespace=${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}

while true; do
  kubectl -n "$namespace" logs -f deploy/notifier --since=1h >>"$out" 2>/dev/null
  sleep 1
done
