#!/usr/bin/env bash
# smoke.sh: both layers, tiny parameters, ~40s total. CI gate for the harness.
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT="${CLAUDE_JOB_DIR:-/tmp}/perf-smoke"
rm -rf "$ROOT"; mkdir -p "$ROOT"
bash scripts/perf/run.sh engine smoke "$ROOT" || exit 1
bash scripts/perf/run.sh service smoke "$ROOT" || exit 1
echo "SMOKE OK"
