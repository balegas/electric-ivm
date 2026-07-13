#!/usr/bin/env bash
# Entrypoint for the single fleet-conformance electric-ivm image (see docs/fleet-conformance.md).
#
# It supervises two processes inside one container:
#   1. durable-streams server (docker/ds-server.ts) — bound to 127.0.0.1 on an internal port,
#      the append log the engine writes to and clients read from.
#   2. the Rust engine (electric-ivm-engine) — binds 0.0.0.0:$ELECTRIC_PORT and serves
#      /v1/shape + /v1/health.
#
# Env translation: the fleet sets Electric's documented vars (DATABASE_URL, ELECTRIC_PORT,
# ELECTRIC_STORAGE, ...). Newer engine builds read ELECTRIC_* directly; to also work with the
# current engine we export the equivalent ELECTRIC_IVM_* fallbacks here (only when unset), so
# both engines behave identically. This redundancy is intentional.
#
# Supervision: if either child exits, the other is killed and the container exits with that code.
# SIGTERM/SIGINT are forwarded to both children so `docker stop` (and tini under `--init`) shuts
# the whole thing down cleanly. Works as PID 1 or PID!=1.
set -euo pipefail

log() { echo "[electric-ivm] $*"; }

# --- resolve config ---------------------------------------------------------

PORT="${ELECTRIC_PORT:-3000}"
DS_PORT="${ELECTRIC_IVM_DS_INTERNAL_PORT:-8791}"
DS_HOST="127.0.0.1"

# Postgres URL: prefer an explicit ELECTRIC_IVM_PG_URL, else the fleet's DATABASE_URL.
PG_URL="${ELECTRIC_IVM_PG_URL:-${DATABASE_URL:-}}"
if [ -z "$PG_URL" ]; then
  log "FATAL: neither DATABASE_URL nor ELECTRIC_IVM_PG_URL is set — nothing to replicate from."
  exit 64
fi

# Storage mode: MEMORY (default) -> in-memory ds, no fsync-per-append cost; FAST_FILE -> file-backed
# under $ELECTRIC_STORAGE_DIR/shapes, durable across restarts. Set ELECTRIC_STORAGE=FAST_FILE
# explicitly for a deployment that must survive a restart without replaying from Postgres.
STORAGE="${ELECTRIC_STORAGE:-MEMORY}"
if [ "$STORAGE" = "MEMORY" ]; then
  export DS_MEMORY=1
  STORAGE_DESC="MEMORY (in-memory durable-streams)"
else
  STORAGE_DIR="${ELECTRIC_STORAGE_DIR:-./persistent}"
  # Anchor a relative dir at the app root so cwd never changes where data lands.
  case "$STORAGE_DIR" in
    /*) : ;;
    *) STORAGE_DIR="${APP_ROOT:-/app}/${STORAGE_DIR#./}" ;;
  esac
  export DS_DATA_DIR="${STORAGE_DIR%/}/shapes"
  mkdir -p "$DS_DATA_DIR"
  # Re-export the resolved absolute dir so the engine's storage sampler (electric.storage.used.bytes,
  # a `du` of ELECTRIC_STORAGE_DIR) measures the real location even when the caller left it unset.
  export ELECTRIC_STORAGE_DIR="$STORAGE_DIR"
  STORAGE_DESC="FAST_FILE (data: $DS_DATA_DIR)"
fi

# durable-streams binds loopback on an internal port; the engine reaches it via ELECTRIC_IVM_DS_URL.
export DS_HOST DS_PORT
export BIND_HOST="$DS_HOST"
: "${ELECTRIC_IVM_DS_URL:=http://${DS_HOST}:${DS_PORT}}"
export ELECTRIC_IVM_DS_URL

# Engine fallbacks (only set what the caller didn't). A newer engine reads ELECTRIC_* directly;
# these keep the current engine working with the same inputs.
export ELECTRIC_IVM_PG_URL="$PG_URL"
: "${ELECTRIC_IVM_BIND:=0.0.0.0:${PORT}}"
export ELECTRIC_IVM_BIND
: "${ELECTRIC_IVM_PG_TABLES:=*}"
export ELECTRIC_IVM_PG_TABLES
if [ -n "${ELECTRIC_LOG_LEVEL:-}" ] && [ -z "${ELECTRIC_IVM_LOG:-}" ]; then
  export ELECTRIC_IVM_LOG="$ELECTRIC_LOG_LEVEL"
fi
if [ -n "${ELECTRIC_REPLICATION_STREAM_ID:-}" ] && [ -z "${ELECTRIC_IVM_PG_SLOT:-}" ]; then
  export ELECTRIC_IVM_PG_SLOT="electric_slot_${ELECTRIC_REPLICATION_STREAM_ID}"
fi

# --- boot log (redacted) ----------------------------------------------------

redact_url() { echo "$1" | sed -E 's#(://[^:/@]+:)[^@/]*@#\1***@#'; }
log "starting fleet-conformance image"
log "  DATABASE_URL       = $(redact_url "$PG_URL")"
log "  ELECTRIC_PORT      = $PORT (engine bind $ELECTRIC_IVM_BIND)"
log "  ELECTRIC_STORAGE   = $STORAGE_DESC"
log "  ELECTRIC_INSTANCE_ID = ${ELECTRIC_INSTANCE_ID:-<generated>}"
log "  ELECTRIC_STATSD_HOST = ${ELECTRIC_STATSD_HOST:-<off>}"
log "  ds internal url    = $ELECTRIC_IVM_DS_URL"
[ -n "${ELECTRIC_SECRET:-}" ] && log "  ELECTRIC_SECRET    = *** (auth required)"

# --- helpers ----------------------------------------------------------------

# Wait until host:port accepts a TCP connection. Returns 1 on timeout, 2 if `pid` died first.
wait_tcp() {
  local host="$1" port="$2" timeout_s="$3" pid="${4:-}"
  local deadline=$(( $(date +%s) + timeout_s ))
  while true; do
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then return 0; fi
    if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then return 2; fi
    if [ "$(date +%s)" -ge "$deadline" ]; then return 1; fi
    sleep 0.2
  done
}

# Parse host:port out of a postgres URL for a pre-flight reachability wait (best-effort).
pg_host_port() {
  local u="${1#*://}"      # drop scheme
  u="${u##*@}"             # drop credentials
  u="${u%%/*}"             # drop /db and query
  u="${u%%\?*}"
  local h="${u%%:*}" p="${u##*:}"
  [ "$p" = "$u" ] && p=5432
  [ -n "$h" ] && echo "$h $p"
}

DS_PID=""
ENGINE_PID=""
SHUTTING_DOWN=0

shutdown() {
  [ "$SHUTTING_DOWN" = 1 ] && return
  SHUTTING_DOWN=1
  trap '' TERM INT
  log "shutting down (forwarding SIGTERM to children)"
  [ -n "$DS_PID" ] && kill -TERM "$DS_PID" 2>/dev/null || true
  [ -n "$ENGINE_PID" ] && kill -TERM "$ENGINE_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  exit 0
}
trap shutdown TERM INT

# --- start durable-streams --------------------------------------------------

log "starting durable-streams on ${DS_HOST}:${DS_PORT} ..."
# Prefer the esbuild-bundled server (image); fall back to tsx for local/dev runs.
if [ -f "${DS_DIR:-/repo/docker}/ds-server.mjs" ]; then
  ( cd "${DS_DIR:-/repo/docker}" && exec node ds-server.mjs ) &
else
  ( cd "${DS_DIR:-/repo/docker}" && exec tsx ds-server.ts ) &
fi
DS_PID=$!

rc=0; wait_tcp "$DS_HOST" "$DS_PORT" 15 "$DS_PID" || rc=$?
if [ "$rc" != 0 ]; then
  [ "$rc" = 2 ] && log "FATAL: durable-streams exited during startup" \
                || log "FATAL: durable-streams did not accept connections within 15s"
  kill -TERM "$DS_PID" 2>/dev/null || true
  exit 1
fi
log "durable-streams ready on ${DS_HOST}:${DS_PORT}"

# --- pre-flight: wait for Postgres to accept TCP (fast when already up) ------

if hp="$(pg_host_port "$PG_URL")"; then
  # shellcheck disable=SC2086
  set -- $hp
  if [ -n "${1:-}" ]; then
    log "waiting for postgres at $1:$2 ..."
    rc=0; wait_tcp "$1" "$2" 30 || rc=$?
    [ "$rc" = 0 ] && log "postgres reachable at $1:$2" \
                  || log "WARN: postgres at $1:$2 not reachable yet — starting engine anyway (it will retry/fail loudly)"
  fi
fi

# --- start engine -----------------------------------------------------------

log "starting engine on 0.0.0.0:${PORT} ..."
electric-ivm-engine &
ENGINE_PID=$!

rc=0; wait_tcp "127.0.0.1" "$PORT" 20 "$ENGINE_PID" || rc=$?
if [ "$rc" != 0 ]; then
  [ "$rc" = 2 ] && log "FATAL: engine exited during startup" \
                || log "FATAL: engine did not bind ${PORT} within 20s"
  kill -TERM "$DS_PID" "$ENGINE_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  exit 1
fi
log "engine listening on 0.0.0.0:${PORT} — /v1/shape and /v1/health are served here"

log "up: durable-streams (pid $DS_PID) + engine (pid $ENGINE_PID)"

# --- supervise: first child to exit brings the container down ---------------

CODE=0
wait -n || CODE=$?
if [ "$SHUTTING_DOWN" = 1 ]; then exit 0; fi
if kill -0 "$ENGINE_PID" 2>/dev/null; then
  log "durable-streams exited (code $CODE); stopping engine"
else
  log "engine exited (code $CODE); stopping durable-streams"
fi
kill -TERM "$DS_PID" "$ENGINE_PID" 2>/dev/null || true
wait 2>/dev/null || true
exit "$CODE"
