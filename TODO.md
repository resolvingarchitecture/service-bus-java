# service-bus — TODO

## Done (1.5.0)

- [x] Bumped `seda-bus` 1.2.0 -> 1.3.1 (event-driven pool, working pub/sub + pause,
      retry + dead-letter, hardened persistence). Java 8 -> 11.
- [x] `Class`-based API: `registerService(Class)`, `registerService(iface, impl)`,
      `startService`/`stopService`/`unregisterService(Class)`,
      `registerAndStartService(s)`, `startAllRegistered`.
- [x] Discovery: `getService(Class)`, `findRunningServices(Class)`,
      `getRunningServices()`, `getRegisteredServices()`, `isRegistered`/`isRunning`,
      name accessors.
- [x] `awaitRunning(timeoutMs, Class...)` - join the async starts.
- [x] Implemented `pause()` / `unpause()` / `restart()` (were no-ops).
- [x] Per-service `ServiceStatus` tracking + `ServiceStatusListener`.
- [x] Fixed `Properties.contains` -> `containsKey`; fixed the `gracefulShutdown`
      wait loop and gave it a timeout.
- [x] `PersistDeadLetter`: size-based rotation (was a stub).
- [x] Reusable `Daemon` base with `configName` / `beforeStart` / `onBusStarted` /
      `onStopping` hooks.
- [x] Real test suite; `README.md`, `DESIGN.md`, `TODO.md`, `CHANGELOG.md`.
- [x] Removed dead code (`availableServices`, `clients`).

## Next

- [ ] `registerAndStartServiceSync(Class, timeoutMs)` convenience (register + start +
      awaitRunning in one call).
- [ ] Return a richer result from registration (an enum / small record) instead of a
      bare boolean, so callers can tell "already registered" from "failed".
- [ ] `dependsOn` ordering for **start**, not just registration (today deps are
      registered first but all starts race).
- [ ] Expose bus metrics (per-channel queue depth, throughput) by surfacing what
      `seda-bus` measures - pairs with seda-bus's planned metrics hooks.
- [ ] Health policy beyond "UNSTABLE -> restart": restart backoff, a give-up
      threshold, `DEGRADED` handling, circuit-breaking a service that keeps failing.
- [ ] Optional readiness gate: hold envelope delivery to a service until it reports
      `RUNNING`/`VERIFIED` (today the consumer is attached at registration, so
      envelopes can arrive mid-startup).
- [ ] `ControlCommand` responses (ack/nack back to the sender with the outcome).
- [ ] Pluggable dead-letter sink (file today; allow a callback / another channel).
- [ ] Interface-vs-impl registration: allow N implementations behind one interface
      with a selection policy (round-robin / primary+fallback).
- [ ] Document the config keys (`ra.servicebus.mbus`, `ra.sedabus.locationBase`, ...).
- [ ] Publish to a real repository (local / jitpack only today).
