#!/usr/bin/env bash
# Run ElectricSQL's own conformance tests against electric-ivm's /v1/shape adapter.
#
# The test files in this directory are Elixir tests that execute INSIDE an ElectricSQL checkout
# (they use Electric's official Electric.Client, OracleHarness/ShapeChecker, and generators).
# This script wires everything up: it locates (or clones) an Electric checkout, copies the test
# files into its sync-service test tree, builds our release engine, and runs the chosen suites.
#
#   electric-conformance/run.sh [oracle|property|subqueries|all]   # default: all
#
# Env:
#   ELECTRIC_DIR    path to an ElectricSQL checkout (default: ../electric next to this repo;
#                   cloned from ELECTRIC_REPO if absent)
#   ELECTRIC_REPO   clone source (default https://github.com/electric-sql/electric)
#   ELECTRIC_REF    optional ref to check out after cloning
#   ORACLE_RUNS / ORACLE_SHAPE_COUNT / ORACLE_BATCH_COUNT / ORACLE_MUTATIONS_PER_TXN
#                   property-test tunables (passed through)
#
# Requirements: elixir/mix, a Rust toolchain, and PostgreSQL binaries (initdb/pg_ctl) on PATH —
# the launcher boots its own ephemeral Postgres.
#
# Note: copying overwrites `test/integration/subquery_*_test.exs` in the Electric checkout with
# the electric-ivm variants (same test bodies, swapped setup) — use a throwaway clone if you
# don't want the checkout modified.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(dirname "$here")"
suite="${1:-all}"

# The tests spawn our launcher (adapter + engine + ephemeral Postgres) through a BEAM port; when
# mix exits, the port's direct child dies but the rest of the chain is orphaned. Sweep anything
# rooted in this repo on exit.
cleanup() {
  pkill -f "$repo/.*electric-adapter.ts" 2>/dev/null || true
  pkill -f "$repo/target/release/electric-ivm-engine" 2>/dev/null || true
}
trap cleanup EXIT

ELECTRIC_REPO="${ELECTRIC_REPO:-https://github.com/electric-sql/electric}"
ELECTRIC_DIR="${ELECTRIC_DIR:-$repo/../electric}"

if [ ! -d "$ELECTRIC_DIR/packages/sync-service" ]; then
  echo "==> cloning $ELECTRIC_REPO -> $ELECTRIC_DIR"
  git clone --depth 1 ${ELECTRIC_REF:+--branch "$ELECTRIC_REF"} "$ELECTRIC_REPO" "$ELECTRIC_DIR"
fi
sync="$ELECTRIC_DIR/packages/sync-service"

echo "==> building the release engine"
(cd "$repo" && cargo build --release -p electric-ivm-engine)

echo "==> copying test files into $sync"
cp "$here/electric_ivm_oracle_test.exs" \
   "$here/electric_ivm_oracle_property_test.exs" \
   "$here/subquery_move_out_test.exs" \
   "$here/subquery_dependency_update_test.exs" \
   "$sync/test/integration/"
cp "$here/el_ivm_setup.ex" "$sync/test/support/"

export ELECTRIC_IVM_DIR="$repo"
cd "$sync"
[ -d deps ] || (echo "==> mix deps.get" && mix deps.get)

case "$suite" in
  oracle)
    mix test test/integration/electric_ivm_oracle_test.exs ;;
  property)
    mix test test/integration/electric_ivm_oracle_property_test.exs ;;
  subqueries)
    mix test test/integration/subquery_move_out_test.exs test/integration/subquery_dependency_update_test.exs ;;
  all)
    mix test test/integration/electric_ivm_oracle_test.exs
    mix test test/integration/electric_ivm_oracle_property_test.exs
    mix test test/integration/subquery_move_out_test.exs test/integration/subquery_dependency_update_test.exs ;;
  *)
    echo "usage: $0 [oracle|property|subqueries|all]"; exit 2 ;;
esac
