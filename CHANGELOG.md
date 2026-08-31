# Service Bus (Java) Changelog

## 1.5.0

Made `service-bus` a proper service-management layer rather than a thin wrapper.

- **Dependency:** `seda-bus` 1.2.0 -> 1.3.1 - the event-driven worker pool (removing
  a ~10 msg/s/channel ceiling), working pub/sub and `pause`, retry + dead-letter,
  hardened persistence. Compile target Java 8 -> 11.
- **`Class`-based API:** `registerService(Class)`, `registerService(iface, impl)`,
  `startService`/`stopService`/`unregisterService(Class)`,
  `registerAndStartService(s)`, `startAllRegistered`.
- **Discovery:** `getService(Class)`, `findRunningServices(Class)` (all running
  services assignable to a type), `getRunningServices()`, `getRegisteredServices()`,
  `isRegistered`/`isRunning`, name accessors.
- **`awaitRunning(timeoutMs, Class...)`** to join the asynchronous service starts.
- **Implemented `pause()` / `unpause()` / `restart()`** (previously returned `false`).
- **Per-service status:** tracked in a map, exposed via `getServiceStatus(Class)` /
  `getServiceStatuses()`, and published to a new `ServiceStatusListener`.
- **Reusable `Daemon`** base class with `configName` / `beforeStart` /
  `onBusStarted` / `onStopping` hooks and a `launch(String[])` lifecycle.
- **Fixes:** `Properties.contains` -> `containsKey` in `start()`; the
  `gracefulShutdown` wait loop (never actually waited) now waits with a timeout;
  `PersistDeadLetter` does size-based rotation instead of growing unbounded.
- Removed dead code (`availableServices` / `listAvailableServices`, `clients`).
- Real test suite; added `README.md`, `DESIGN.md`, `TODO.md`.

## 1.4.0

- Prior release. Thin wrapper over `seda-bus:1.2.0`: reflective registry,
  `ControlCommand` handling, dependency-ordered registration, dead-letter file.
