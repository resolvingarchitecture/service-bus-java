<div align="center">
  <h1>service-bus (Java)</h1>
  <p><strong>Resolving Architecture &mdash; Clarity in Design</strong></p>
  <p>Lifecycle management and discovery for a set of services over a message bus.</p>
</div>

`service-bus` sits on top of [`seda-bus`](https://github.com/resolvingarchitecture/seda-bus-java):
`seda-bus` moves envelopes between named channels and walks their routing slips;
`service-bus` gives you **services** as the unit of composition &mdash; register them,
start/stop/pause them, let them find each other, and watch their health.

Each service becomes one `seda-bus` channel keyed by its class name, with the service
as that channel's consumer. A service orchestrates others by pushing routes onto the
envelope's `DynamicRoutingSlip`; the bus does the rest.

Dependencies: `resolvingarchitecture:common` and `resolvingarchitecture:seda-bus`.

## Use

```java
ServiceBus bus = new ServiceBus(properties);
bus.start(properties);

bus.registerAndStartServices(IdentityService.class, RoutingService.class);
bus.awaitRunning(5_000, IdentityService.class, RoutingService.class);

// discovery
RoutingService router = bus.getService(RoutingService.class);
List<ProtocolService> transports = bus.findRunningServices(ProtocolService.class);

// send
Envelope e = Envelope.documentFactory();
e.addRoute(RoutingService.class, "ROUTE");
e.ratchet();
bus.send(e);                         // fire and forget
bus.send(e, reply -> { /* ... */ }); // with an end-of-slip callback

bus.gracefulShutdown();
```

### As a daemon

```java
public class MyDaemon extends ra.servicebus.Daemon {
    public static void main(String[] args) { new MyDaemon().launch(args); }

    protected String configName() { return "my.config"; }
    protected void beforeStart(Properties config) { /* dirs, logging, time zone */ }
    protected void onBusStarted(ServiceBus bus, Properties config) {
        bus.registerAndStartServices(FooService.class, BarService.class);
    }
}
```

`launch` loads config, runs the hooks, installs a shutdown hook, and idles until the
bus stops.

## API

| area          | methods                                                                 |
|---------------|------------------------------------------------------------------------|
| register      | `registerService(Class)`, `registerService(iface, impl)`, `registerAndStartService(s)` |
| lifecycle     | `startService`, `stopService`, `startAllRegistered`, `pause`/`unpause`, `restart`, `shutdown`/`gracefulShutdown` |
| discovery     | `getService(Class)`, `findRunningServices(Class)`, `getRunningServices()`, `isRegistered`/`isRunning` |
| wait          | `awaitRunning(timeoutMs, Class...)`                                     |
| health        | `getServiceStatus(Class)`, `getServiceStatuses()`, `registerServiceStatusListener` |
| bus           | `getStatus()`, `registerBusStatusListener`                              |
| control       | send an `Envelope` with `commandPath` = a `ControlCommand` (`RegisterService`, `StartService`, ...) |

The `String`-based `registerService(...)` overloads remain for the `ServiceRegistrar`
contract and `ControlCommand` handling.

## Behaviour notes

- **Dependency-ordered registration** &mdash; a service's `getServicesDependentUpon()`
  entries are registered first.
- **Async start** &mdash; `startService` returns immediately; the service starts on its
  own thread. Use `awaitRunning` to join.
- **UNSTABLE auto-restart** &mdash; a service reporting `ServiceStatus.UNSTABLE` is
  restarted on a background thread.
- **Dead letters** &mdash; `deadLetter(Envelope)` appends JSON (one per line) to
  `<baseDir>/deadLetter.json`, rotating past 5 MB, keeping 5 files.
- **Graceful shutdown** stops each service on its own thread and waits up to 30 s
  (5 s for a hard `shutdown()`), then stops the bus.

## Build

```sh
mvn test        # needs common:1.2.0 and seda-bus:1.3.1 in the local repo
mvn package
```

## Reference

- [`DESIGN.md`](DESIGN.md) &mdash; the model and the seams
- [`TODO.md`](TODO.md) &mdash; what's next
- [`CHANGELOG.md`](CHANGELOG.md)
