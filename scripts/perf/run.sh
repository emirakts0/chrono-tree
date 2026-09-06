#!/usr/bin/env bash
# run.sh <engine|service> <scenario> <results-root>
# Scenarios: baseline rate-100k rate-200k sym-1000 sym-2000 trickle
#            burst-100k burst-500k corner smoke smoke-trickle
set -uo pipefail
cd "$(dirname "$0")/../.."

LAYER="$1"; SCN="$2"; ROOT="${3:-docs/perf/2026-09-06-campaign}"
# TRICKLE_BAND: ladder span as a fraction of Ref. 0.002 (the old value)
# sits deep inside the walk's stationary band (σ≈0.8%/1.6%/4.7% per
# decimals class), so at campaign scale the whole ladder fired in the
# first seconds. 0.6 keeps fires sparse and sustained: measured on
# scripts/benchengine (1M alerts, 500 syms, 20k tps) 96783 fires over 60s
# and 138298 (13.8% of the ladder) over the full 240s, ~77 of 2000 over
# the 10s smoke.
ALERTS=1000000; SYMBOLS=500; RATE=20000; LAYOUT=parked; CLUSTER=0; DUR=240s
TRICKLE_BAND=0.6
case "$SCN" in
  baseline)   ;;
  rate-100k)  RATE=100000 ;;
  rate-200k)  RATE=200000 ;;
  sym-1000)   SYMBOLS=1000 ;;
  sym-2000)   SYMBOLS=2000 ;;
  trickle)    LAYOUT=trickle ;;
  burst-100k) LAYOUT=gap; CLUSTER=100000 ;;
  burst-500k) LAYOUT=gap; CLUSTER=500000 ;;
  corner)     LAYOUT=gap; CLUSTER=100000; RATE=200000; SYMBOLS=2000 ;;
  smoke)      ALERTS=1000; SYMBOLS=200; RATE=5000; DUR=10s ;;
  smoke-trickle) LAYOUT=trickle; ALERTS=2000; SYMBOLS=200; RATE=5000; DUR=10s ;;
  *) echo "unknown scenario $SCN" >&2; exit 2 ;;
esac

DIR="$ROOT/$LAYER-$SCN"
mkdir -p "$DIR"
LOG="$DIR/run.log"
echo "=== $LAYER/$SCN layout=$LAYOUT alerts=$ALERTS symbols=$SYMBOLS rate=$RATE cluster=$CLUSTER dur=$DUR ===" | tee "$LOG"

if [ "$LAYER" = engine ]; then
  mkdir -p "$ROOT/.bin"
  go build -o "$ROOT/.bin/benchengine" ./scripts/benchengine
  "$ROOT/.bin/benchengine" -scenario "$SCN" -layout "$LAYOUT" -alerts "$ALERTS" \
    -symbols "$SYMBOLS" -cluster "$CLUSTER" -band "$TRICKLE_BAND" \
    -rate "$RATE" -duration "$DUR" -out "$DIR" 2>&1 | tee -a "$LOG"
  go tool pprof -top -nodecount=25 "$ROOT/.bin/benchengine" "$DIR/cpu.pprof" > "$DIR/pprof-top.txt" 2>>"$LOG" || true
  grep -q '"valid": true' "$DIR/summary.json"; RC=$?
else
  TMP="$DIR/tmp"; mkdir -p "$TMP"
  go run ./scripts/benchfeed -mode seed -db "$TMP/alerts.bbolt" -scenario "$SCN" \
    -layout "$LAYOUT" -alerts "$ALERTS" -symbols "$SYMBOLS" -cluster "$CLUSTER" \
    -band "$TRICKLE_BAND" -rate "$RATE" -out "$DIR" 2>&1 | tee -a "$LOG"
  go build -o "$TMP/chronod" ./cmd/chronod
  "$TMP/chronod" -nats-url "" -db "$TMP/alerts.bbolt" -grpc-addr :19090 -http-addr :18080 \
    > "$TMP/chronod.log" 2>&1 &
  CPID=$!
  # 1 Hz stats sampler (runs the whole scenario, captures replay + load +
  # drain) and RSS cap: >8 GiB kills chronod before the box OOMs. Each
  # sample is stamped with an epoch-seconds ts (jq) so perfcollect can
  # window the CPU mean to the load phase via the load_start/load_end
  # markers below. jq is also the cap check; without it, log once and
  # sample untimestamped and uncapped (perfcollect falls back to the
  # whole-log mean).
  RSS_CAP=$(( 8 * 1024 * 1024 * 1024 ))
  HAVE_TS=0
  if command -v jq >/dev/null 2>&1; then
    HAVE_JQ=1; HAVE_TS=1
  else
    HAVE_JQ=0
    echo "jq not found — sampling untimestamped, skipping RSS cap check" | tee -a "$LOG"
  fi
  ( while kill -0 $CPID 2>/dev/null; do
      S=$(curl -sf localhost:18080/stats) || { sleep 1; continue; }
      if [ "$HAVE_TS" = 1 ]; then
        if ! S=$(printf '%s' "$S" | jq -c --argjson ts "$(date +%s)" '. + {ts: $ts}'); then
          HAVE_TS=0
          echo "stats timestamping failed — falling back to untimestamped samples" >> "$LOG"
        fi
      fi
      echo "$S" >> "$DIR/stats.jsonl"
      if [ "$HAVE_JQ" = 1 ] && echo "$S" | jq -e --argjson cap $RSS_CAP '.sys.rss_bytes > $cap' >/dev/null 2>&1; then
        echo "RSS CAP EXCEEDED — killing chronod" >> "$LOG"; kill $CPID
      fi
      sleep 1
    done ) &
  SPID=$!
  # Wait for boot (replay of 1M alerts can take minutes — untimed).
  # /healthz, not /readyz: readyz stays 503 until a feed has connected
  # (feed-stale gating), so waiting on it before starting benchfeed
  # would spin forever.
  until curl -sf localhost:18080/healthz >/dev/null; do
    kill -0 $CPID 2>/dev/null || { echo "chronod died during boot" | tee -a "$LOG"; break; }
    sleep 1
  done
  # Mid-run 30s CPU profile: start it at half the load phase (clamped to
  # load start — the 10s smoke cannot center a 30s window).
  DURS=$(( ${DUR%s} ))
  PSTART=$(( DURS/2 - 15 )); [ $PSTART -lt 0 ] && PSTART=0
  ( sleep $PSTART; curl -sf "localhost:18080/debug/pprof/profile?seconds=30" > "$DIR/cpu.pprof" ) &
  PPID2=$!
  # Load-phase markers: perfcollect windows the CPU mean (and RSS plateau)
  # to samples between these epoch seconds, so boot replay and post-load
  # idle stop diluting the measurement.
  date +%s > "$DIR/load_start"
  go run ./scripts/benchfeed -mode feed -server localhost:19090 -stats localhost:18080 -scenario "$SCN" \
    -layout "$LAYOUT" -alerts "$ALERTS" -symbols "$SYMBOLS" -cluster "$CLUSTER" \
    -band "$TRICKLE_BAND" -rate "$RATE" -duration "$DUR" -out "$DIR" 2>&1 | tee -a "$LOG"
  FEEDRC=$?
  date +%s > "$DIR/load_end"
  curl -sf localhost:18080/debug/pprof/heap > "$DIR/heap.pprof" || echo "heap profile failed" | tee -a "$LOG"
  curl -sf "localhost:18080/debug/pprof/goroutine?debug=1" > "$DIR/goroutine.txt" || true
  # Let the 30s CPU profile finish before killing chronod — on the short
  # smoke scenario it is still sampling when the feed ends.
  wait $PPID2 2>/dev/null
  kill $CPID 2>/dev/null; wait $CPID 2>/dev/null; kill $SPID 2>/dev/null
  go run ./scripts/perfcollect -dir "$DIR" 2>&1 | tee -a "$LOG"
  go tool pprof -top -nodecount=25 "$TMP/chronod" "$DIR/cpu.pprof" > "$DIR/pprof-top.txt" 2>>"$LOG" || true
  rm -rf "$TMP"
  grep -q '"valid": true' "$DIR/summary.json"; RC=$?
fi
if [ $RC -eq 0 ]; then echo "RUN $LAYER/$SCN: VALID" | tee -a "$LOG"
else echo "RUN $LAYER/$SCN: INVALID (see summary.json)" | tee -a "$LOG"; fi
exit $RC
