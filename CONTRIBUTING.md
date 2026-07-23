# Contributing

Thanks for your interest in Electric Circuits. Issues and pull requests are welcome.

## Getting started

Read the [README](README.md) for what the project is, and [AGENTS.md](AGENTS.md) for the
repository layout, the docs index, and the invariants the engine must hold. The docs it points
to (`docs/ARCHITECTURE.md`, `docs/ivm-engine-internals.md`) explain the design; read them
before changing the engine.

## Build and test

You need Node 20+, pnpm, Rust (pinned by `rust-toolchain.toml`), and Docker (for Postgres).

```bash
pnpm install
pnpm engine:test                               # Rust unit + integration (fast)
ELECTRIC_CIRCUITS_ENGINE_PREBUILT=1 pnpm test  # full vitest suite incl. oracle conformance
```

A change that touches the engine must pass both suites. The full runbook — conformance against
Electric's own oracle, the demo stack, the visualizer — is in the "Build & test" and "Testing
checklist" sections of AGENTS.md.

## Pull requests

- Keep each PR to one change; small PRs merge faster.
- Say what the change does and why; link the issue if one exists.
- Add or extend tests for behavior you change — the conformance suite is the safety net, but
  regressions should be caught closer to the code.

## License

This project is dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
Unless you state otherwise, any contribution you submit is licensed under the same terms,
with no additional conditions.
