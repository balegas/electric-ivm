//! Regression guard for the StatsD system sampler's process-CPU measurement.
//!
//! `vm.scheduler_utilization.total` divides the process CPU% by the core count. sysinfo only reports
//! a real process CPU value under a specific refresh pattern: `System::new_all()` (which seeds the
//! per-process CPU-time baseline) followed by a targeted `refresh_processes_specifics(..).with_cpu()`
//! with `refresh_cpu_all()` first. A plain `System::new()` + targeted refresh reads 0 forever, which
//! would make the engine emit a fake, always-zero utilization — a "never emit unmeasured" violation.
//! This test pins the working pattern so a future sysinfo bump or refactor can't silently regress it.

use std::time::{Duration, Instant};

fn burn(until: Instant) {
    let mut x: u64 = 0;
    while Instant::now() < until {
        x = x.wrapping_add(1).wrapping_mul(2654435761);
    }
    std::hint::black_box(x);
}

#[test]
fn sampler_pattern_measures_process_cpu_under_load() {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

    let pid = sysinfo::get_current_pid().expect("current pid");
    let proc_cpu = ProcessRefreshKind::nothing().with_cpu();

    // Mirror statsd::system_sampler exactly: new_all() baseline, then refresh_cpu_all + targeted
    // process refresh, two samples spaced over the CPU load.
    let mut sys = System::new_all();
    sys.refresh_cpu_all();
    sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, proc_cpu);

    // Keep the process busy across the sampling interval (> sysinfo's minimum) on this thread.
    burn(Instant::now() + Duration::from_millis(400));

    sys.refresh_cpu_all();
    sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, proc_cpu);

    let cpu = sys.process(pid).map(|p| p.cpu_usage()).unwrap_or(0.0);
    assert!(cpu > 0.0, "process CPU must be measured (>0) under sustained load, got {cpu}");
}
