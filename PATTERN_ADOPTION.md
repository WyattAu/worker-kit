# Pattern adoption: worker-kit

Background job supervisor with leases, triggers, and pressure awareness.
Declared per `pattern-standards/references/adoption-format.md`.

| Pattern | Status | Evidence |
|---|---|---|
| T12 | adopted | src/supervisor.rs:158 (WorkerSupervisor registers jobs, tracks JobStatus and RunReport, and tears down via ShutdownGuard) |
| P51 | adopted | src/supervisor.rs:220 (CircuitBreakerConfig wired per supervisor) |
| P105 | adopted | src/trigger.rs:25 (cron trigger: a run outlasting its interval fires the next immediately after; UTC-based, jittered) and tests/cron_schedule.rs:1 |
| P62 | adopted | src/supervisor.rs:210 (drain_cap) and src/supervisor.rs:177 (ShutdownGuard) |
| T34 | partial | src/leader/redis_lease.rs:25 (SET NX PX acquisition with compare-and-delete; fencing tokens are documented as the caller's responsibility, not enforced here) |
| T36 | partial | src/supervisor.rs:198 (seed) and src/trigger.rs:204 (JitterPolicy, clock_seed) make runs deterministic; no full simulation harness |
