# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

## Git Policy

Do not commit or push unless explicitly asked. At handoff, report changed files, validation run,
and suggested next commands.

## Build & Test

```bash
pnpm engine:test                          # Rust unit + integration (fast)
ELECTRIC_CIRCUITS_ENGINE_PREBUILT=1 pnpm test  # full vitest suite incl. oracle conformance (boots its own Postgres)
ASDF_ELIXIR_VERSION=1.18.4-otp-28 ASDF_ERLANG_VERSION=28.1 \
  ./electric-conformance/run.sh oracle    # Electric's own oracle vs /v1/shape (needs elixir + ../electric)
pnpm demo:linearlite                      # demo stack: PG + engine + LinearLite + pipeline visualizer
```

**Finishing an engine-touching task requires all three suites green, plus driving the demo
(browser e2e) for live-path/visualizer changes — see "Testing checklist before claiming done"
in AGENTS.md.**

Full commands, the demo/visualizer runbook (incl. driving the visualizer with the Playwright MCP),
invariants, and gotchas live in **AGENTS.md** — read it before touching the engine or the apps.

## Architecture Overview

See AGENTS.md (layout + docs index) and `docs/ARCHITECTURE.md`.

## Conventions & Patterns

See the Invariants and Gotchas sections of AGENTS.md.
