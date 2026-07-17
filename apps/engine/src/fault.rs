//! TEST-ONLY fault injection.
//!
//! Reads `ELECTRIC_CIRCUITS_FAULT` once at startup. With the env var unset — the default in every
//! real deployment and every normal test run — [`active`] returns [`Fault::None`] and this module
//! has **zero** effect on engine behaviour. It exists solely so the conformance suite can prove,
//! via a negative control, that the oracle harness actually catches engine bugs (a green suite is
//! only meaningful if a deliberately-wrong engine makes it go red).

use std::sync::OnceLock;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fault {
    /// Normal, correct behaviour.
    None,
    /// Never emit shape "leave" (delete) envelopes, so a row that exits a shape lingers in the
    /// client forever. Injected in `engine::translate_output`.
    DropDeletes,
    /// Treat `>=`/`<=` as strict `>`/`<`, so rows exactly on a boundary literal are mishandled.
    /// Injected in `predicate::cmp`.
    OffByOneCmp,
}

fn detect() -> Fault {
    match std::env::var("ELECTRIC_CIRCUITS_FAULT").ok().as_deref() {
        Some("drop_deletes") => Fault::DropDeletes,
        Some("off_by_one_cmp") => Fault::OffByOneCmp,
        _ => Fault::None,
    }
}

/// The active fault for this process (read once from the environment, then cached).
pub fn active() -> Fault {
    static F: OnceLock<Fault> = OnceLock::new();
    *F.get_or_init(detect)
}
