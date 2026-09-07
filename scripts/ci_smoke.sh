#!/usr/bin/env bash
# Fresh ephemeral runner only: this destroys its own containers at exit, retains volumes.
set -euo pipefail
mkdir -p artifacts/ci
compose=(docker compose -f deployment/compose.yaml)
finish() {
  status=$?
  "${compose[@]}" ps -a > artifacts/ci/compose-status.txt 2>&1 || true
  "${compose[@]}" logs --no-color --tail 300 > artifacts/ci/compose.log 2>&1 || true
  curl --fail --silent http://localhost:18081/jobs/overview > artifacts/ci/jobs.json || true
  curl --fail --silent http://localhost:18081/taskmanagers > artifacts/ci/taskmanagers.json || true
  if [[ -d artifacts/drills ]]; then cp -a artifacts/drills artifacts/ci/drills; fi
  "${compose[@]}" down --timeout 20 >> artifacts/ci/cleanup.log 2>&1 || true
  exit "$status"
}
# Prevent accidental use against a developer's existing live cluster.
if [[ -n "$("${compose[@]}" ps -aq)" ]]; then
  echo 'Refusing CI teardown workflow against an existing AdPulse cluster.' >&2
  exit 2
fi
trap finish EXIT
python - <<'PY' > artifacts/ci/environment.json
import json,os,platform
print(json.dumps({'environment':'GitHub-hosted runner' if os.getenv('GITHUB_ACTIONS') else 'local ephemeral runner',
 'run_id':os.getenv('GITHUB_RUN_ID'), 'commit':os.getenv('GITHUB_SHA'), 'os':platform.platform(),
 'cpu_count':os.cpu_count(), 'cloud_service_deployment':False}))
PY
"${compose[@]}" up -d --build --wait --wait-timeout 420 > artifacts/ci/startup.log 2>&1
.venv/bin/python scripts/integration.py --users 80 --output artifacts/ci/integration
.venv/bin/python scripts/disk_reconcile.py --output artifacts/ci/disk-reconciliation.json
"${compose[@]}" exec -T inspection-coverage python -m adpulse.inspection coverage --once --wait-lock
"${compose[@]}" exec -T inspection-metrics python -m adpulse.inspection metrics --once --wait-lock
.venv/bin/python scripts/inspection_acceptance.py --output artifacts/ci/inspection.json --timeout 180
.venv/bin/python -m adpulse.cli replay --from-s3 --release ci-verified --publish --output artifacts/ci/replay
.venv/bin/python scripts/query_acceptance.py --release ci-verified --output artifacts/ci/query.json
.venv/bin/python scripts/sink_acceptance.py --output artifacts/ci/sink-acceptance.json
.venv/bin/python scripts/drills.py --scenario worker-restart
.venv/bin/python scripts/drills.py --scenario sink-replay
