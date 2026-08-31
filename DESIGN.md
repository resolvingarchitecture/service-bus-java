# service-bus — Design

## Role

`seda-bus` is a transport: named channels, a worker pool, bounded queues, the
routing-slip engine. It has no notion of a "service".

`service-bus` adds that notion and everything that comes with managing a set of them:

- **composition** — a `ra.common.service.BaseService` is the unit; each becomes one
  channel + its consumer;
- **lifecycle** — register, start, stop, pause, restart, for individual services and
  for the whole bus, with dependency ordering;
- **discovery** — services and hosts look each other up by type without a bus
  round-trip;
- **health** — per-service `ServiceStatus` is tracked, published to listeners, and an
  `UNSTABLE` service is auto-restarted;
- **remote control** — a `ControlCommand` envelope registers/starts/stops services;
- **failure sink** — dead letters go to a rotating file.

Anything an application-specific bus (e.g. `1m5-core`) would otherwise re-implement
belongs here.

## Model

    ServiceBus  implements MessageProducer, LifeCycle, ServiceRegistrar, Runnable
      mBus                MessageBus (a SEDABus by default; override with
                          ra.servicebus.mbus)
      registeredServices  name -> BaseService        (ConcurrentHashMap)
      runningServices     name -> BaseService
      serviceStatuses     name -> ServiceStatus
      busStatusListeners / serviceStatusListeners

    register(name, impl):
      instantiate impl reflectively (public no-arg ctor)
      register its dependencies first
      service.setProducer(this); service.setObserver(this)
      mBus.registerChannel(name); mBus.registerAsynchConsumer(name, service)

    start(name):  new AppThread -> service.start(config) -> runningServices.put(...)
    send(e):      if e.commandPath -> processCommand(e);  mBus.publish(e[, client])

"name" is the class name: the registration key, the channel name, and the value a
routing slip's `Route.getService()` carries.

## Why the class name is the key

It makes routing trivial: a producer writes `envelope.addRoute(FooService.class,
"OP")`, `seda-bus` resolves the channel by that string, and the `FooService`
instance (registered as that channel's consumer) handles it. No separate address
space, no registry lookup on the hot path.

Trade-off: one implementation per name. Registering an implementation under an
interface name (`registerService(FooApi.class, FooImpl.class)`) is supported for the
cases that need it.

## Discovery

`getService(Class)` returns the instance registered under that exact class.
`findRunningServices(Class type)` returns every running service assignable to `type`
— this is how a router finds all `ProtocolService`s, or a supervisor finds all
`DataService`s, without those categories meaning anything to `service-bus` itself.
The category vocabulary lives in the application layer; `service-bus` only offers the
`isInstance` query.

## Async start and `awaitRunning`

`startService` returns immediately and starts the service on an `AppThread` (services
may block on network or disk during startup). `awaitRunning(timeoutMs, Class...)`
blocks until the named services (or all registered, if none named) are in
`runningServices`. Daemons call it after `onBusStarted`; tests call it before
asserting.

## Status and self-healing

Services call `updateStatus(ServiceStatus)` (in `BaseService`), which reaches
`ServiceBus.serviceStatusChanged`. The bus records it in `serviceStatuses`, forwards
it to every `ServiceStatusListener`, and on `UNSTABLE` spawns a restart.

`BaseService.updateStatus` also emits a `SERVICE_STATUS` event routed to
`ra.notification.NotificationService` — hosts that want that pub-sub must register a
NotificationService; the direct `ServiceStatusListener` path here does not need one.

## Control commands

Send an `Envelope` whose `commandPath` is a `ra.common.network.ControlCommand`
(`RegisterService`, `UnregisterService`, `StartService`, `StopService`,
`GracefullyStopService`) with `serviceClass` / `interfaceClass` NVPs. `ServiceBus`
is a `MessageProducer` handed to every service, so a service can reconfigure the bus
by sending it a command.

## Shutdown

`shutdown()` / `gracefulShutdown()` stop each running service on its own thread, wait
up to a bounded time (5 s / 30 s) for `runningServices` to drain, then stop the
underlying bus. `pause()` pauses every running service and the bus;
`unpause()` reverses it; `restart()` is `shutdown()` then `start(config)`.

## Not here

- No priority or weighting between services (that is `seda-bus` per-stage config).
- No distributed registry — one JVM, one bus.
- No hot code reload — `restart()` re-runs `start()` on the same instance.
