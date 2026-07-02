#!/usr/bin/env bash
# Boot LinearLite at a chosen workload size — all-inclusive. One command starts the whole stack
# (Postgres + logical replication, the durable-streams log, the Rust engine, the API, the LinearLite
# web UI, AND the shape/dbsp pipeline explorer); another tears it all down.
#
# The workload size is a number of ISSUES (or a named tier); the number of USERS and PROJECTS is derived
# from it, and the seeded roster scales to match (the "Viewing as" switcher and project list adapt).
#
#   scripts/linearlite.sh start <size>   size = small | medium | large | xlarge | <number-of-issues>
#   scripts/linearlite.sh stop
#   scripts/linearlite.sh status
#
# Env: DEMO_HTTPS_PORT (webui, 8443)  DEMO_VIZ_PORT (explorer, 5180)  EL_LOG (log file)
#      DEMO_VIZ=0 to skip the explorer.

set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG="${EL_LOG:-/tmp/el-linearlite.log}"
HTTPS_PORT="${DEMO_HTTPS_PORT:-8443}"
VIZ_PORT="${DEMO_VIZ_PORT:-5180}"
WEBUI="https://localhost:${HTTPS_PORT}/"
VIZ="http://localhost:${VIZ_PORT}/"

usage() {
  sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

# ISSUES -> (USERS, PROJECTS): users grows ~sqrt(issues), projects ~users/2.5, with sensible floors.
derive() {
  USERS=$(awk -v i="$1" 'BEGIN{u=int(sqrt(i)/4+0.5); if(u<6)u=6; print u}')
  PROJECTS=$(awk -v u="$USERS" 'BEGIN{p=int(u/2.5+0.5); if(p<5)p=5; print p}')
}

start() {
  local size="${1:-medium}"
  case "$size" in
    small) ISSUES=1000 ;;
    medium) ISSUES=20000 ;;
    large) ISSUES=100000 ;;
    xl | xlarge) ISSUES=500000 ;;
    '' | *[!0-9]*) echo "size must be small|medium|large|xlarge or a number of issues (got '$size')"; exit 1 ;;
    *) ISSUES="$size" ;;
  esac
  derive "$ISSUES"

  if lsof -ti :"${HTTPS_PORT}" >/dev/null 2>&1; then
    echo "port ${HTTPS_PORT} is already in use — run '$0 stop' first (or set DEMO_HTTPS_PORT)."; exit 1
  fi

  echo "starting LinearLite  ·  ${ISSUES} issues  ·  ${USERS} users  ·  ${PROJECTS} projects  (web UI + dbsp explorer)"
  (
    cd "$ROOT" &&
      DEMO_SEED_COUNT="$ISSUES" DEMO_USERS="$USERS" DEMO_PROJECTS="$PROJECTS" \
        DEMO_HTTPS_PORT="$HTTPS_PORT" DEMO_VIZ_PORT="$VIZ_PORT" \
        nohup pnpm demo:linearlite >"$LOG" 2>&1 &
    echo $! >/tmp/el-linearlite.pid
  )

  printf 'booting'
  for _ in $(seq 1 150); do
    if grep -q "Open a URL above" "$LOG" 2>/dev/null; then break; fi
    if grep -qE "startup failed|not found|Error:" "$LOG" 2>/dev/null; then
      echo; echo "boot failed — see $LOG:"; grep -vE "warning|Compiling|Finished" "$LOG" | tail -6; exit 1
    fi
    printf '.'; sleep 2
  done
  echo
  grep -E "primed" "$LOG" 2>/dev/null | tail -1
  cat <<EOF

  🖥  LinearLite web UI      →  ${WEBUI}
  🔬 dbsp pipeline explorer  →  ${VIZ}

  logs: ${LOG}     stop with: $0 stop
EOF
}

stop() {
  echo "tearing down LinearLite…"
  # 1. graceful: let start.ts run its own shutdown (drops the replication slot, stops PG, removes the
  #    ephemeral data dir, kills its engine/caddy/vite/viz children).
  pkill -TERM -f "examples/linearlite/start.ts" 2>/dev/null
  for _ in $(seq 1 10); do lsof -ti :"${HTTPS_PORT}" >/dev/null 2>&1 || break; sleep 1; done
  # 2. force-clean anything left (idempotent; scoped to the linearlite demo).
  pkill -9 -f "examples/linearlite/start.ts" 2>/dev/null
  pkill -9 -f "filter @electric-ivm/linearlite" 2>/dev/null
  pkill -9 -f "pipeline-viz" 2>/dev/null
  lsof -ti :"${HTTPS_PORT}" 2>/dev/null | while read -r p; do kill -9 "$p" 2>/dev/null; done
  lsof -ti :"${VIZ_PORT}" 2>/dev/null | while read -r p; do kill -9 "$p" 2>/dev/null; done
  for p in $(pgrep -f "el-linearlite-pg-.*/data" 2>/dev/null); do kill -9 "$p" 2>/dev/null; done
  rm -f /tmp/el-linearlite.pid
  echo "stopped."
}

status() {
  if lsof -ti :"${HTTPS_PORT}" >/dev/null 2>&1; then
    echo "running  ·  web UI ${WEBUI}  ·  explorer ${VIZ}"
    grep -E "primed" "$LOG" 2>/dev/null | tail -1
  else
    echo "not running"
  fi
}

cmd="${1:-}"
case "$cmd" in
  start) start "${2:-medium}" ;;
  stop) stop ;;
  status) status ;;
  *) usage; exit 1 ;;
esac
