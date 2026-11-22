#!/usr/bin/env bash
# Changes the Aggregator during a sustained k6 run. Start it together with SCENARIO=sustained.
# scale:   P2 criterion 6 moves from 3 to 5 to 2 instances.
# rollout: P2 criterion 8 swaps the image to the given tag. Pair it with SCENARIO=rollout.
set -eu

namespace=sistem-informasi-kebencanaan-bnpb
mode=${1:?usage: scale.sh scale | scale.sh rollout TAG}
step=${STEP_SECONDS:-40}

log() { echo "$(date -u +%H:%M:%S) $*"; }

case "$mode" in
scale)
  sleep "$step"
  log "aggregator to 5 instances"
  kubectl -n "$namespace" scale deployment/aggregator --replicas=5
  sleep "$step"
  log "aggregator to 2 instances"
  kubectl -n "$namespace" scale deployment/aggregator --replicas=2
  ;;
rollout)
  tag=${2:?usage: scale.sh rollout TAG}
  sleep "$step"
  log "aggregator image to $tag"
  kubectl -n "$namespace" set image deployment/aggregator "aggregator=atlas/aggregator:$tag"
  kubectl -n "$namespace" rollout status deployment/aggregator --timeout=5m
  ;;
*)
  echo "unknown mode $mode" >&2
  exit 2
  ;;
esac

log "done"
