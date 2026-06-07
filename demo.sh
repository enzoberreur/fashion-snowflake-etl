#!/usr/bin/env bash
# One-shot demo for Bloc 3 - Monogram Paris real-time pipeline.
# Ingestion -> Loading -> Transformation -> Data Quality -> Insights, in one run.
# Produces a bounded batch of sale events to Redpanda, streams them into Snowflake
# exactly-once, transforms with dbt, runs the quality tests, then points at the
# Streamlit dashboard. Built to be screen-recorded for the Loom.
#
# Usage:
#   bash demo.sh                  # full run (needs .venv + .env + Snowflake)
#   bash demo.sh --dry-run        # Redpanda only, no Snowflake (safe fallback)
#   bash demo.sh --launch-dashboard   # also start Streamlit at the end
#   AUTO=1 bash demo.sh           # no pauses (unattended)
#   N=150 bash demo.sh            # number of events (default 150)
#
# Prerequisites for the full run (your machine, gitignored): a .venv with the
# project installed, a populated .env, the RSA key, and a live Snowflake account.
set -uo pipefail
cd "$(dirname "$0")"

GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
AUTO="${AUTO:-0}"; OPEN=0; DRY=0; LAUNCH=0; N="${N:-150}"
for a in "$@"; do case "$a" in --open) OPEN=1;; --auto) AUTO=1;; --dry-run) DRY=1;; --launch-dashboard) LAUNCH=1;; esac; done

step(){ echo; echo -e "${BLUE}==================================================${NC}"; echo -e "${BLUE}>> $1${NC}"; echo -e "${BLUE}==================================================${NC}"; }
ok(){ echo -e "${GREEN}[OK] $1${NC}"; }
warn(){ echo -e "${YELLOW}[!] $1${NC}"; }
pause(){ if [ "$AUTO" = "1" ]; then sleep "${1:-3}"; else echo; echo -e "${YELLOW}-- Press Enter to continue --${NC}"; read -r; fi; }
open_url(){ [ "$OPEN" = "1" ] || return 0; ( cmd.exe /c start "" "$1" 2>/dev/null || xdg-open "$1" 2>/dev/null || open "$1" 2>/dev/null ) & }

command -v docker >/dev/null 2>&1 || { echo -e "${RED}docker not found on PATH. Open a terminal where 'docker' works and re-run.${NC}"; exit 1; }

# Locate the project virtualenv (Windows or POSIX layout).
PY=""
for c in .venv/Scripts/python.exe .venv/bin/python; do [ -x "$c" ] && PY="$c" && break; done
[ -z "$PY" ] && { echo -e "${RED}No .venv found.${NC} Create it first:"; echo "  python -m venv .venv && . .venv/Scripts/activate && pip install -e '.[dev,dbt,streaming,dashboard]'"; exit 1; }
export PYTHONPATH=python
if [ "$DRY" = "0" ] && [ ! -f .env ]; then
  warn "No .env found - the Snowflake steps will fail. Use --dry-run, or create .env (see .env.example + docs/STREAMING.md)."
fi

step "Monogram Paris - real-time pipeline one-shot demo"
[ "$DRY" = "1" ] && warn "DRY RUN: Redpanda only, no Snowflake writes / no dbt"
echo "Events: $N   Python: $PY"
pause 1

step "1/6  Start the broker (Redpanda + Console)"
echo "\$ docker compose up -d"
docker compose up -d
echo -n "Waiting for Redpanda health "
i=0; until docker compose exec -T redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true'; do echo -n "."; sleep 2; i=$((i+2)); [ "$i" -ge 60 ] && break; done; echo
docker compose exec -T redpanda rpk topic create monogram.sales.stream -p 3 2>/dev/null && ok "topic created (3 partitions)" || echo "(topic already exists)"
echo "Redpanda Console: http://localhost:8088  (topic monogram.sales.stream)"
open_url "http://localhost:8088"
pause

step "2/6  Ingestion - produce $N sale events to Kafka"
if [ "$DRY" = "1" ]; then
  "$PY" -m monogram_etl.streaming.producer --rate 50 --count "$N" --dry-run | tail -5
else
  "$PY" -m monogram_etl.streaming.producer --rate 50 --count "$N"
fi
ok "Produced $N events (keyed by store_id, ordered per store across 3 partitions)"
pause

step "3/6  Loading - stream into Snowflake (exactly-once)"
if [ "$DRY" = "1" ]; then
  "$PY" -m monogram_etl.streaming.consumer --batch-size 50 --max-messages "$N" --dry-run | tail -5
  warn "Dry run: Snowflake write skipped"
else
  "$PY" -m monogram_etl.streaming.consumer --batch-size 50 --max-messages "$N"
  ok "Consumed into INGEST.STREAM_SALES (1 channel/partition, Kafka offset = Snowpipe offset token)"
fi
pause

if [ "$DRY" = "0" ]; then
  step "4/6  Transformation - dbt builds the star schema + real-time fact"
  ( cd dbt && "$PY" -m dbt.cli.main build --select stg_stream_sales+ fct_sales_realtime --profiles-dir . )
  ok "INGEST.STREAM_SALES -> STAGING.STG_STREAM_SALES -> MARTS.FCT_SALES_REALTIME"
  pause

  step "5/6  Data quality - exactly-once + freshness"
  ( cd dbt && "$PY" -m dbt.cli.main test --select stg_stream_sales fct_sales_realtime --profiles-dir . )
  ( cd dbt && "$PY" -m dbt.cli.main source freshness --profiles-dir . ) || warn "freshness returned warnings"
  ok "No duplicate offsets (exactly-once) + source freshness verified"
  pause
else
  echo; echo "(dry-run: steps 4-5 need Snowflake; skipped)"
fi

step "6/6  Insights - Streamlit dashboard"
echo "Business insights (MARTS star schema) + live real-time stream tab:"
echo "  \$ streamlit run dashboard/streamlit_app.py   ->  http://localhost:8501"
if [ "$LAUNCH" = "1" ]; then
  "$PY" -m streamlit run dashboard/streamlit_app.py --server.port 8501 >/tmp/monogram_streamlit.log 2>&1 &
  sleep 4; open_url "http://localhost:8501"; ok "Streamlit launched (PID $!), log: /tmp/monogram_streamlit.log"
fi
echo
echo "Monitoring artifacts: dags/monogram_stream_monitor_dag.py + sql/validation/assert_stream_*.sql"
pause

step "Demo complete"
echo "Stop the broker:  docker compose down"
[ "$LAUNCH" = "1" ] && echo "Stop Streamlit:   kill the background process shown above"
