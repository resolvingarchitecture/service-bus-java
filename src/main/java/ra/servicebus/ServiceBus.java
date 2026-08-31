package ra.servicebus;

import ra.common.AppThread;
import ra.common.Client;
import ra.common.Config;
import ra.common.Envelope;
import ra.common.LifeCycle;
import ra.common.Status;
import ra.common.SystemSettings;
import ra.common.Wait;
import ra.common.messaging.MessageBus;
import ra.common.messaging.MessageProducer;
import ra.common.network.ControlCommand;
import ra.common.service.BaseService;
import ra.common.service.Service;
import ra.common.service.ServiceNotAccessibleException;
import ra.common.service.ServiceNotSupportedException;
import ra.common.service.ServiceRegistrar;
import ra.common.service.ServiceStatus;
import ra.sedabus.SEDABus;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

/**
 * Manages the lifecycle of a set of {@link Service}s over a {@link MessageBus}.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>register services (reflectively, dependency-ordered) - each becomes one bus
 *       channel keyed by its class name, with the service as the channel's consumer;</li>
 *   <li>start / stop / pause / restart services and the underlying bus;</li>
 *   <li>let services and hosts <b>discover</b> each other ({@link #getService},
 *       {@link #findRunningServices}, {@link #getRunningServices});</li>
 *   <li>observe per-service {@link ServiceStatus} and auto-restart an
 *       {@code UNSTABLE} service; publish status to {@link ServiceStatusListener}s;</li>
 *   <li>accept {@link ControlCommand} envelopes so the bus is controllable over
 *       itself;</li>
 *   <li>route dead letters to a file.</li>
 * </ul>
 *
 * <p>Prefer the {@code Class}-based overloads ({@link #registerService(Class)},
 * {@link #registerAndStartService(Class)}, {@link #findRunningServices(Class)}); the
 * {@code String}-based methods exist for the {@link ServiceRegistrar} contract and
 * for {@link ControlCommand} handling.
 */
public final class ServiceBus implements MessageProducer, LifeCycle, ServiceRegistrar, Runnable {

    private static final Logger LOG = Logger.getLogger(ServiceBus.class.getName());

    private volatile Status status = Status.Stopped;

    private Properties config;
    private MessageBus mBus;
    private String deadLetterFilePath;

    private final Map<String, BaseService> registeredServices = new ConcurrentHashMap<>();
    private final Map<String, BaseService> runningServices = new ConcurrentHashMap<>();
    private final Map<String, ServiceStatus> serviceStatuses = new ConcurrentHashMap<>();

    private final List<BusStatusListener> busStatusListeners = new CopyOnWriteArrayList<>();
    private final List<ServiceStatusListener> serviceStatusListeners = new CopyOnWriteArrayList<>();

    public ServiceBus(Properties config) {
        this.config = config;
    }

    @Override
    public void run() {
        start(config);
    }

    // ------------------------------------------------------------------
    // MessageProducer
    // ------------------------------------------------------------------

    @Override
    public boolean send(Envelope e) {
        if (e == null) { LOG.warning("Envelope is required."); return false; }
        if (e.getCommandPath() != null) processCommand(e);
        return mBus.publish(e);
    }

    @Override
    public boolean send(Envelope e, Client client) {
        if (e == null) { LOG.warning("Envelope is required."); return false; }
        if (e.getCommandPath() != null) processCommand(e);
        return mBus.publish(e, client);
    }

    @Override
    public boolean deadLetter(Envelope envelope) {
        new Thread(new PersistDeadLetter(envelope, deadLetterFilePath), "ServiceBus-DeadLetter").start();
        return true;
    }

    private void processCommand(Envelope e) {
        ControlCommand cc;
        try {
            cc = ControlCommand.valueOf(e.getCommandPath());
        } catch (IllegalArgumentException iae) {
            LOG.warning("Unknown ControlCommand: " + e.getCommandPath());
            return;
        }
        LOG.info("Received command (" + cc.name() + ") for service bus...");
        switch (cc) {
            case RegisterService: {
                String interfaceClass = (String) e.getValue("interfaceClass");
                String serviceClass = (String) e.getValue("serviceClass");
                try {
                    if (interfaceClass == null) registerService(serviceClass, config);
                    else registerService(interfaceClass, serviceClass, config);
                } catch (ServiceNotAccessibleException x) {
                    e.addErrorMessage("Service " + serviceClass + " not accessible; not registered.");
                } catch (ServiceNotSupportedException x) {
                    e.addErrorMessage("Service " + serviceClass + " not supported; not registered.");
                }
                break;
            }
            case UnregisterService:
                unregisterService((String) e.getValue("serviceClass"));
                break;
            case StartService:
                startService((String) e.getValue("serviceClass"));
                break;
            case StopService:
                stopService((String) e.getValue("serviceClass"), false);
                break;
            case GracefullyStopService:
                stopService((String) e.getValue("serviceClass"), true);
                break;
            default:
                LOG.warning("ControlCommand not handled by ServiceBus: " + cc.name());
        }
    }

    // ------------------------------------------------------------------
    // Registration - String (ServiceRegistrar contract + ControlCommand)
    // ------------------------------------------------------------------

    @Override
    public boolean registerService(String serviceName, Properties p)
            throws ServiceNotAccessibleException, ServiceNotSupportedException {
        return registerService(serviceName, serviceName, p);
    }

    public boolean registerService(String interfaceName, String serviceName, Properties p)
            throws ServiceNotAccessibleException, ServiceNotSupportedException {
        if (registeredServices.containsKey(interfaceName)) {
            LOG.info("Already registered, skipping: " + interfaceName);
            return true;
        }
        LOG.info("Registering " + interfaceName + " -> " + serviceName);
        if (p != null && !p.isEmpty()) config.putAll(p);
        try {
            final BaseService service = (BaseService) Class.forName(serviceName).getConstructor().newInstance();
            // register dependencies first
            List<String> deps = service.getServicesDependentUpon();
            if (deps != null) {
                for (String c : deps) registerService(c, config);
            }
            service.setProducer(this);
            service.setObserver(this);
            mBus.registerChannel(interfaceName);
            mBus.registerAsynchConsumer(interfaceName, service);
            registeredServices.put(interfaceName, service);
            serviceStatuses.put(interfaceName, ServiceStatus.NOT_INITIALIZED);
            service.setRegistered(true);
            LOG.info("Registered: " + serviceName);
            return true;
        } catch (InstantiationException e) {
            LOG.warning(e.toString());
            throw new ServiceNotSupportedException(e);
        } catch (IllegalAccessException e) {
            LOG.warning(e.toString());
            throw new ServiceNotAccessibleException(e);
        } catch (NoSuchMethodException | InvocationTargetException | ClassNotFoundException e) {
            LOG.warning("Cannot register " + serviceName + ": " + e);
            return false;
        }
    }

    @Override
    public boolean unregisterService(String serviceName) {
        final BaseService service = registeredServices.get(serviceName);
        if (service == null) return true;
        new AppThread(() -> {
            if (service.shutdown()) {
                registeredServices.remove(serviceName);
                runningServices.remove(serviceName);
                serviceStatuses.remove(serviceName);
                service.setRegistered(false);
                LOG.info("Unregistered: " + serviceName);
            }
        }, serviceName + "-ShutdownThread").start();
        return true;
    }

    public boolean startService(String serviceName) {
        final BaseService service = registeredServices.get(serviceName);
        if (service == null) {
            LOG.warning("Not registered, cannot start: " + serviceName);
            return false;
        }
        if (runningServices.containsKey(serviceName)) return true;
        new AppThread(() -> {
            if (service.start(config)) {
                runningServices.put(serviceName, service);
                LOG.info("Running: " + serviceName);
            } else {
                LOG.warning("Failed to start: " + serviceName);
            }
        }, serviceName + "-StartupThread").start();
        return true;
    }

    public boolean stopService(String serviceName, boolean gracefully) {
        final BaseService service = runningServices.get(serviceName);
        if (service == null) return true;
        new AppThread(() -> {
            boolean ok = gracefully ? service.gracefulShutdown() : service.shutdown();
            if (ok) {
                runningServices.remove(serviceName);
                LOG.info((gracefully ? "Gracefully stopped: " : "Stopped: ") + serviceName);
            }
        }, serviceName + "-ShutdownThread").start();
        return true;
    }

    // ------------------------------------------------------------------
    // Registration - Class-based (preferred)
    // ------------------------------------------------------------------

    /** Register a service class (interface == implementation). Catches and logs. */
    public boolean registerService(Class<? extends Service> serviceClass) {
        return registerService(serviceClass, serviceClass);
    }

    /** Register an implementation under an interface/type name. Catches and logs. */
    public boolean registerService(Class<? extends Service> interfaceClass, Class<? extends Service> implClass) {
        try {
            return registerService(interfaceClass.getName(), implClass.getName(), config);
        } catch (Exception e) {
            LOG.warning("registerService(" + implClass.getName() + "): " + e.getMessage());
            return false;
        }
    }

    public boolean startService(Class<? extends Service> serviceClass) {
        return startService(serviceClass.getName());
    }

    public boolean stopService(Class<? extends Service> serviceClass, boolean gracefully) {
        return stopService(serviceClass.getName(), gracefully);
    }

    public boolean unregisterService(Class<? extends Service> serviceClass) {
        return unregisterService(serviceClass.getName());
    }

    /** Register then start. Returns false if registration failed. */
    public boolean registerAndStartService(Class<? extends Service> serviceClass) {
        return registerService(serviceClass) && startService(serviceClass);
    }

    @SafeVarargs
    public final void registerAndStartServices(Class<? extends Service>... serviceClasses) {
        for (Class<? extends Service> c : serviceClasses) registerAndStartService(c);
    }

    /** Start every registered service that is not yet running. */
    public void startAllRegistered() {
        for (String name : registeredServices.keySet()) {
            if (!runningServices.containsKey(name)) startService(name);
        }
    }

    // ------------------------------------------------------------------
    // Discovery
    // ------------------------------------------------------------------

    public Set<String> getRegisteredServiceNames() {
        return Collections.unmodifiableSet(registeredServices.keySet());
    }

    public Set<String> getRunningServiceNames() {
        return Collections.unmodifiableSet(runningServices.keySet());
    }

    public Collection<BaseService> getRegisteredServices() {
        return Collections.unmodifiableCollection(registeredServices.values());
    }

    public Collection<BaseService> getRunningServices() {
        return Collections.unmodifiableCollection(runningServices.values());
    }

    /** The service registered under this exact class/name, running or not, or null. */
    @SuppressWarnings("unchecked")
    public <T extends Service> T getService(Class<T> serviceClass) {
        BaseService s = registeredServices.get(serviceClass.getName());
        return s != null && serviceClass.isInstance(s) ? (T) s : null;
    }

    /** All running services assignable to the given type (class or interface). */
    public <T> List<T> findRunningServices(Class<T> type) {
        List<T> out = new ArrayList<>();
        for (BaseService s : runningServices.values()) {
            if (type.isInstance(s)) out.add(type.cast(s));
        }
        return out;
    }

    public boolean isRegistered(Class<? extends Service> serviceClass) {
        return registeredServices.containsKey(serviceClass.getName());
    }

    public boolean isRunning(Class<? extends Service> serviceClass) {
        return runningServices.containsKey(serviceClass.getName());
    }

    public ServiceStatus getServiceStatus(Class<? extends Service> serviceClass) {
        return serviceStatuses.get(serviceClass.getName());
    }

    public Map<String, ServiceStatus> getServiceStatuses() {
        return Collections.unmodifiableMap(serviceStatuses);
    }

    /**
     * Block until the given services are running (or all registered services, if
     * none are named), or the timeout elapses.
     *
     * @return true if every target reached running before the timeout
     */
    public boolean awaitRunning(long timeoutMs, Class<?>... serviceClasses) {
        Collection<String> targets;
        if (serviceClasses.length == 0) {
            targets = new ArrayList<>(registeredServices.keySet());
        } else {
            targets = new ArrayList<>();
            for (Class<?> c : serviceClasses) targets.add(c.getName());
        }
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (runningServices.keySet().containsAll(targets)) return true;
            Wait.aMs(50);
        }
        return runningServices.keySet().containsAll(targets);
    }

    // ------------------------------------------------------------------
    // Status observation
    // ------------------------------------------------------------------

    public void registerBusStatusListener(BusStatusListener l) { busStatusListeners.add(l); }
    public void unregisterBusStatusListener(BusStatusListener l) { busStatusListeners.remove(l); }

    public void registerServiceStatusListener(ServiceStatusListener l) { serviceStatusListeners.add(l); }
    public void unregisterServiceStatusListener(ServiceStatusListener l) { serviceStatusListeners.remove(l); }

    @Override
    public void serviceStatusChanged(String serviceFullName, ServiceStatus serviceStatus) {
        LOG.info("Service (" + serviceFullName + ") -> " + serviceStatus.name());
        serviceStatuses.put(serviceFullName, serviceStatus);
        for (ServiceStatusListener l : serviceStatusListeners) {
            try {
                l.serviceStatusChanged(serviceFullName, serviceStatus);
            } catch (RuntimeException e) {
                LOG.warning("ServiceStatusListener: " + e.getMessage());
            }
        }
        if (serviceStatus == ServiceStatus.UNSTABLE) {
            BaseService service = registeredServices.get(serviceFullName);
            if (service != null) {
                LOG.warning("Service (" + serviceFullName + ") UNSTABLE; restarting...");
                new AppThread(service::restart, serviceFullName + "-RestartThread").start();
            }
        }
    }

    private void updateStatus(Status status) {
        this.status = status;
        LOG.info("RA Service Bus: " + status.name());
        for (BusStatusListener l : busStatusListeners) {
            try {
                l.busStatusChanged(status);
            } catch (RuntimeException e) {
                LOG.warning("BusStatusListener: " + e.getMessage());
            }
        }
    }

    // ------------------------------------------------------------------
    // LifeCycle
    // ------------------------------------------------------------------

    @Override
    public boolean start(Properties properties) {
        updateStatus(Status.Starting);
        try {
            this.config = Config.loadAll(properties, "ra-servicebus.config");
        } catch (Exception e) {
            LOG.warning("config: " + e.getMessage());
            this.config = properties != null ? properties : new Properties();
        }

        File baseLocDir;
        if (this.config.containsKey("ra.sedabus.locationBase")) {
            baseLocDir = new File(this.config.getProperty("ra.sedabus.locationBase"));
        } else {
            try {
                baseLocDir = SystemSettings.getUserAppDataDir(".ra", getClass().getName(), true);
            } catch (IOException e) {
                LOG.severe(e.getMessage());
                updateStatus(Status.Errored);
                return false;
            }
        }
        if (!baseLocDir.exists() && !baseLocDir.mkdirs()) {
            LOG.severe("cannot create " + baseLocDir);
            updateStatus(Status.Errored);
            return false;
        }
        File deadLetterFile = new File(baseLocDir, "deadLetter.json");
        try {
            if (!deadLetterFile.exists() && !deadLetterFile.createNewFile()) {
                LOG.severe("cannot create " + deadLetterFile);
                updateStatus(Status.Errored);
                return false;
            }
        } catch (IOException e) {
            LOG.severe(e.getMessage());
            updateStatus(Status.Errored);
            return false;
        }
        deadLetterFilePath = deadLetterFile.getAbsolutePath();

        String mBusType = this.config.getProperty("ra.servicebus.mbus");
        if (mBusType != null) {
            try {
                mBus = (MessageBus) Class.forName(mBusType).getConstructor().newInstance();
            } catch (Exception e) {
                LOG.severe("mbus " + mBusType + ": " + e.getMessage());
                updateStatus(Status.Errored);
                return false;
            }
        } else {
            mBus = new SEDABus();
        }
        mBus.start(this.config);

        updateStatus(Status.Running);
        return true;
    }

    @Override
    public boolean pause() {
        if (status != Status.Running) return false;
        for (BaseService s : runningServices.values()) s.pause();
        mBus.pause();
        updateStatus(Status.Paused);
        return true;
    }

    @Override
    public boolean unpause() {
        if (status != Status.Paused) return false;
        mBus.unpause();
        for (BaseService s : runningServices.values()) s.unpause();
        updateStatus(Status.Running);
        return true;
    }

    @Override
    public boolean restart() {
        Properties saved = this.config;
        return shutdown() && start(saved);
    }

    @Override
    public boolean shutdown() {
        return doShutdown(false, 5_000L);
    }

    @Override
    public boolean gracefulShutdown() {
        return doShutdown(true, 30_000L);
    }

    private boolean doShutdown(boolean graceful, long serviceTimeoutMs) {
        updateStatus(Status.Stopping);
        List<String> names = new ArrayList<>(runningServices.keySet());
        for (final String name : names) {
            final BaseService service = runningServices.get(name);
            if (service == null) continue;
            AppThread t = new AppThread(() -> {
                boolean ok = graceful ? service.gracefulShutdown() : service.shutdown();
                if (ok) runningServices.remove(name);
            }, name + (graceful ? "-GracefulShutdownThread" : "-ShutdownThread"));
            t.setDaemon(true);
            t.start();
        }
        long deadline = System.currentTimeMillis() + serviceTimeoutMs;
        while (!runningServices.isEmpty() && System.currentTimeMillis() < deadline) {
            Wait.aMs(100);
        }
        if (!runningServices.isEmpty()) {
            LOG.warning("services did not stop within " + serviceTimeoutMs + "ms: " + runningServices.keySet());
        }
        boolean busOk = graceful ? mBus.gracefulShutdown() : mBus.shutdown();
        updateStatus(busOk ? Status.Stopped : Status.Errored);
        return busOk && runningServices.isEmpty();
    }

    public Status getStatus() {
        return status;
    }

    /** The underlying message bus. Rarely needed by callers. */
    public MessageBus messageBus() {
        return mBus;
    }
}
