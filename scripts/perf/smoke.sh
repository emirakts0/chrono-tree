#!/usr/bin/env bash
# smoke.sh: both layers, tiny parameters, ~1min total. CI gate for the
# harness — parked (zero-fire) AND trickle (some-but-not-all) on each
# layer, so tier parity and the firing band are exercised end to end.
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT="${CLAUDE_JOB_DIR:-/tmp}/perf-smoke"
rm -rf "$ROOT"; mkdir -p "$ROOT"
bash scripts/perf/run.sh engine smoke "$ROOT" || exit 1
bash scripts/perf/run.sh service smoke "$ROOT" || exit 1
bash scripts/perf/run.sh engine smoke-trickle "$ROOT" || exit 1
bash scripts/perf/run.sh service smoke-trickle "$ROOT" || exit 1
echo "SMOKE OK"
