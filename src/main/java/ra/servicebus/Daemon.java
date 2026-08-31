package ra.servicebus;

import ra.common.Config;
import ra.common.Status;
import ra.common.Wait;

import java.util.Properties;
import java.util.logging.Logger;

/**
 * A reusable headless host for a {@link ServiceBus}.
 *
 * <p>Run it standalone ({@code java ra.servicebus.Daemon key=value ...}) or subclass
 * it and override the hooks to build an application daemon:
 *
 * <pre>{@code
 * public class MyDaemon extends Daemon {
 *     public static void main(String[] args) { new MyDaemon().launch(args); }
 *     protected String configName() { return "my.config"; }
 *     protected void beforeStart(Properties config) { ... prepare dirs ... }
 *     protected void onBusStarted(ServiceBus bus, Properties config) {
 *         bus.registerAndStartServices(FooService.class, BarService.class);
 *     }
 * }
 * }</pre>
 *
 * {@link #launch(String[])} loads config, calls {@link #beforeStart}, starts the bus,
 * calls {@link #onBusStarted}, installs a shutdown hook, then idles until the bus
 * stops or errors.
 */
public class Daemon {

    private static final Logger LOG = Logger.getLogger(Daemon.class.getName());

    private volatile ServiceBus bus;
    private volatile boolean shuttingDown;

    public static void main(String[] args) {
        new Daemon().launch(args);
    }

    public final void launch(String[] args) {
        Properties config = loadConfig(args);
        beforeStart(config);

        bus = new ServiceBus(config);
        if (!bus.start(config)) {
            LOG.severe("ServiceBus failed to start.");
            return;
        }
        onBusStarted(bus, config);

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "ServiceBus-Daemon-Shutdown"));
        LOG.info(getClass().getSimpleName() + " running.");

        while (bus.getStatus() != Status.Stopped && bus.getStatus() != Status.Errored) {
            Wait.aSec(1);
        }
    }

    public final synchronized void shutdown() {
        if (shuttingDown) return;
        shuttingDown = true;
        // The JVM's java.util.logging shutdown hook may have already closed the
        // console handler by the time our hook runs, so mirror shutdown-path
        // messages to stdout.
        shutdownLog(getClass().getSimpleName() + " shutting down...");
        try {
            onStopping();
        } catch (RuntimeException e) {
            shutdownLog("onStopping() failed: " + e);
        }
        if (bus != null) {
            boolean ok = bus.gracefulShutdown();
            shutdownLog("bus stopped=" + ok);
        }
    }

    private void shutdownLog(String msg) {
        LOG.info(msg);
        System.out.println("[" + getClass().getSimpleName() + "] " + msg);
    }

    public final ServiceBus bus() {
        return bus;
    }

    // -- hooks -----------------------------------------------------------

    /** Classpath config file name. Default {@code ra-servicebus.config}. */
    protected String configName() {
        return "ra-servicebus.config";
    }

    protected Properties loadConfig(String[] args) {
        try {
            return Config.loadFromMainArgsAndClasspath(args, configName(), false);
        } catch (Exception e) {
            LOG.warning("config load: " + e.getMessage() + "; falling back to args only");
            return Config.loadFromMainArgs(args);
        }
    }

    /** Runs before the bus is created (prepare directories, logging, time zone). */
    protected void beforeStart(Properties config) {
    }

    /** Runs after the bus is Running - register and start services here. */
    protected void onBusStarted(ServiceBus bus, Properties config) {
    }

    /** Runs at the start of shutdown, before the bus is stopped. */
    protected void onStopping() {
    }
}
