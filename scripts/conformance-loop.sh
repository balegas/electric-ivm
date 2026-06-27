#!/usr/bin/env bash
# Agent conformance loop: run the oracle-driven fuzz test repeatedly until it fails (or until
# ITER iterations pass). Each iteration uses a fresh random base seed and many random-predicate
# shapes. On failure the test output prints `FAILED seed=<n>`; replay it exactly with:
#
#     SEED=<n> pnpm exec vitest run packages/conformance/src/conformance-fuzz.test.ts
#
# Usage: scripts/conformance-loop.sh [iterations]   (default 50)
set -uo pipefail
cd "$(dirname "$0")/.."

ITER="${1:-50}"
pnpm exec vitest --version >/dev/null 2>&1 || { echo "install deps first: pnpm install"; exit 2; }

for i in $(seq 1 "$ITER"); do
  echo "=== conformance loop ${i}/${ITER} ==="
  if ! pnpm exec vitest run packages/conformance/src/conformance-fuzz.test.ts; then
    echo ""
    echo "CONFORMANCE FAILED on iteration ${i}. See the 'FAILED seed=' line above and replay with:"
    echo "  SEED=<n> pnpm exec vitest run packages/conformance/src/conformance-fuzz.test.ts"
    exit 1
  fi
done
echo "All ${ITER} conformance iterations passed."
