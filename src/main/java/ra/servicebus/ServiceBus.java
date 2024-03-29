package ra.servicebus;

import ra.common.Client;
import ra.common.Envelope;
import ra.common.LifeCycle;
import ra.common.Status;
import ra.common.messaging.MessageBus;
import ra.common.messaging.MessageProducer;
import ra.common.network.ControlCommand;
import ra.common.service.*;
import ra.sedabus.SEDABus;
import ra.common.AppThread;
import ra.common.Config;
import ra.common.SystemSettings;
import ra.common.Wait;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 */
public final class ServiceBus implements MessageProducer, LifeCycle, ServiceRegistrar, Runnable {

    private static final Logger LOG = Logger.getLogger(ServiceBus.class.getName());

    private Status status = Status.Stopped;

    private Properties config;

    private MessageBus mBus;

    private List<String> availableServices;
    private Map<String, BaseService> registeredServices;
    private Map<String, BaseService> runningServices;

    private Map<String, Client> clients;

    private String deadLetterFilePath;

    private final List<BusStatusListener> busStatusListeners = new ArrayList<>();

    public ServiceBus(Properties config) {
        this.config = config;
    }

    @Override
    public void run() {
        start(config);
    }

    @Override
    public boolean send(Envelope e) {
        if(e==null) {
            LOG.warning("Envelope is required.");
            return false;
        }
//        LOG.info("Received envelope.");
        if(e.getCommandPath()!=null) {
            processCommand(e);
        }
        return mBus.publish(e);
    }

    @Override
    public boolean send(Envelope e, Client client) {
        if(e==null) {
            LOG.warning("Envelope is required.");
            return false;
        }
//        LOG.info("Received envelope with client.");
        if(e.getCommandPath()!=null) {
            processCommand(e);
        }
        return mBus.publish(e, client);
    }

    private void processCommand(Envelope e) {
        ControlCommand cc = ControlCommand.valueOf(e.getCommandPath());
        LOG.info("Received command ("+e.getCommandPath()+") for service bus...");
        switch (cc) {
            case RegisterService: {
                String interfaceClass = (String)e.getValue("interfaceClass");
                String serviceClass = (String)e.getValue("serviceClass");
                try {
                    if(interfaceClass==null)
                        registerService(serviceClass, config);
                    else
                        registerService(interfaceClass, serviceClass, config);
                } catch (ServiceNotAccessibleException serviceNotAccessibleException) {
                    e.addErrorMessage("Interface: "+interfaceClass+"; Service: "+serviceClass+" Not Accessible by Service Bus. Unable to Register.");
                } catch (ServiceNotSupportedException serviceNotSupportedException) {
                    e.addErrorMessage("Interface: "+interfaceClass+"; Service: "+serviceClass+" Not Supported by Service Bus. Unable to Register.");
                }
                break;
            }
            case UnregisterService: {
                String serviceClass = (String)e.getValue("serviceClass");
                unregisterService(serviceClass);
                break;
            }
            case StartService: {
                String serviceClass = (String)e.getValue("serviceClass");
                startService(serviceClass);
                break;
            }
            case StopService: {
                String serviceClass = (String)e.getValue("serviceClass");
                stopService(serviceClass, false);
                break;
            }
            case GracefullyStopService: {
                String serviceClass = (String)e.getValue("serviceClass");
                stopService(serviceClass, true);
                break;
            }
        }
    }

    @Override
    public boolean deadLetter(Envelope envelope) {
        new Thread(new PersistDeadLetter(envelope, deadLetterFilePath)).start();
        return true;
    }

    public void registerBusStatusListener (BusStatusListener busStatusListener) {
        busStatusListeners.add(busStatusListener);
    }

    public void unregisterBusStatusListener(BusStatusListener busStatusListener) {
        busStatusListeners.remove(busStatusListener);
    }

    public List<String> listAvailableServices() {
        return availableServices;
    }

    public boolean registerService(String serviceName, Properties p) throws ServiceNotAccessibleException, ServiceNotSupportedException {
        return registerService(serviceName, serviceName, p);
    }

    public boolean registerService(String interfaceName, String serviceName, Properties p) throws ServiceNotAccessibleException, ServiceNotSupportedException {
        if(registeredServices.containsKey(interfaceName)) {
            LOG.info("Service already registered, skipping: interface class is "+interfaceName+ " with service class: "+serviceName);
            return true;
        }
        LOG.info("Registering interface class: "+interfaceName+" with service class: "+serviceName);
        if(p != null && p.size() > 0)
            config.putAll(p);
        try {
            final BaseService service = (BaseService)Class.forName(serviceName).getConstructor().newInstance();
            // Ensure dependent services are registered
            if(service.getServicesDependentUpon()!=null && service.getServicesDependentUpon().size() > 0) {
                for(String c : service.getServicesDependentUpon()) {
                    registerService(c, config);
                }
            }
            // Continue registering this service
            service.setProducer(this);
            service.setObserver(this);
            mBus.registerChannel(interfaceName);
            mBus.registerAsynchConsumer(interfaceName, service);
            // register service
            registeredServices.put(interfaceName, service);
            service.setRegistered(true);

            LOG.info("Service registered successfully: "+serviceName);
        } catch (InstantiationException e) {
            LOG.warning(e.getLocalizedMessage());
            throw new ServiceNotSupportedException(e);
        } catch (IllegalAccessException e) {
            LOG.warning(e.getLocalizedMessage());
            throw new ServiceNotAccessibleException(e);
        } catch (NoSuchMethodException e) {
            LOG.warning(e.getLocalizedMessage());
            return false;
        } catch (InvocationTargetException e) {
            LOG.warning(e.getLocalizedMessage());
            return false;
        } catch (ClassNotFoundException e) {
            LOG.warning(e.getLocalizedMessage());
            return false;
        }
        return true;
    }

    public boolean unregisterService(String serviceName) {
        if(registeredServices.containsKey(serviceName)) {
            final BaseService service = registeredServices.get(serviceName);
            new AppThread(new Runnable() {
                @Override
                public void run() {
                    if(service.shutdown()) {
                        registeredServices.remove(serviceName);
                        service.setRegistered(false);
                        LOG.info("Service unregistered successfully: "+serviceName);
                    }
                }
            }, serviceName+"-ShutdownThread").start();
        }
        return true;
    }

    public boolean startService(String serviceName) {
        // init registered service
        if(registeredServices.containsKey(serviceName)) {
            final BaseService service = registeredServices.get(serviceName);
            new AppThread(new Runnable() {
                @Override
                public void run() {
                    if (service.start(config)) {
                        runningServices.put(serviceName, service);
                        LOG.info("Service registered successfully as running: " + serviceName);
                    } else {
                        LOG.warning("Registered service failed to start: " + serviceName);
                    }
                }
            }, serviceName + "-StartupThread").start();
        } else {
            return false;
        }
        return true;
    }

    public boolean stopService(String serviceName, boolean gracefully) {
        if(runningServices.containsKey(serviceName)) {
            final BaseService service = runningServices.get(serviceName);
            new AppThread(new Runnable() {
                @Override
                public void run() {
                    if(gracefully && service.gracefulShutdown()) {
                        runningServices.remove(serviceName);
                        LOG.info("Service gracefully shutdown and unregistered successfully: "+serviceName);
                    } else if(!gracefully && service.shutdown()) {
                        runningServices.remove(serviceName);
                        LOG.info("Service quick shutdown and unregistered successfully: "+serviceName);
                    }
                }
            }, serviceName+"-ShutdownThread").start();
        }
        return true;
    }

    private void updateStatus(Status status) {
        this.status = status;
        switch(status) {
            case Starting: {
                LOG.info("RA Service Bus is Starting");
                break;
            }
            case Running: {
                LOG.info("RA Service Bus is Running");
                break;
            }
            case Stopping: {
                LOG.info("RA Service Bus is Stopping");
                break;
            }
            case Stopped: {
                LOG.info("RA Service Bus has Stopped");
                break;
            }
            case Errored: {
                LOG.warning("RA Service Bus has errored.");
                break;
            }
        }
        LOG.info("Updating Bus Status Listeners; size="+busStatusListeners.size());
        for(BusStatusListener l : busStatusListeners) {
            l.busStatusChanged(status);
        }
    }

    public void serviceStatusChanged(String serviceFullName, ServiceStatus serviceStatus) {
        LOG.info("Service ("+serviceFullName+") reporting new status("+serviceStatus.name()+") to Bus.");
        switch(serviceStatus) {
            case UNSTABLE: {
                // Service is Unstable - restart
                BaseService service = registeredServices.get(serviceFullName);
                if(service != null) {
                    LOG.warning("Service ("+serviceFullName+") reporting UNSTABLE; restarting...");
                    service.restart();
                }
                break;
            }
            case RUNNING: {
                LOG.fine("Service ("+serviceFullName+") reporting Running.");
                break;
            }
            case SHUTDOWN: {
                LOG.fine("Service ("+serviceFullName+") reporting Shutdown.");
                break;
            }
            case GRACEFULLY_SHUTDOWN: {
                LOG.fine("Service ("+serviceFullName+") reporting Gracefully Shutdown.");
                break;
            }
        }
    }

    /**
     * Starts up Service Bus registering internal services, starting all services registered, and starting message channel
     * and worker thread pool.
     *
     * @param properties
     * @return
     */
    @Override
    public boolean start(Properties properties) {
        updateStatus(Status.Starting);
        try {
            this.config = Config.loadAll(properties, "ra-servicebus.config");
        } catch (Exception e) {
            LOG.warning(e.getLocalizedMessage());
            this.config = properties;
        }
        String baseLocation;
        File baseLocDir;
        if(properties.contains("ra.sedabus.locationBase")) {
            baseLocation = properties.getProperty("ra.sedabus.locationBase");
            baseLocDir = new File(baseLocation);
        } else {
            try {
                baseLocDir = SystemSettings.getUserAppDataDir(".ra", this.getClass().getName(), true);
                baseLocation = baseLocDir.getAbsolutePath();
            } catch (IOException e) {
                LOG.severe(e.getLocalizedMessage());
                return false;
            }
        }
        if(!baseLocDir.exists() && !baseLocDir.mkdir()) {
            LOG.severe("Unable to start Service Bus due to unable to create base directory: " + baseLocation);
            return false;
        }
        File deadLetterFile = new File(baseLocDir, "deadLetter.json");
        try {
            if(!deadLetterFile.exists() && !deadLetterFile.createNewFile()) {
                LOG.severe("Unable to start Service Bus due to unable to create dead letter file: " + baseLocDir.getAbsolutePath() + "/deadLetter.json");
                return false;
            }
        } catch (IOException e) {
            LOG.severe(e.getLocalizedMessage());
            return false;
        }
        deadLetterFilePath = deadLetterFile.getAbsolutePath();

        String mBusType = properties.getProperty("ra.servicebus.mbus");
        if(mBusType!=null) {
            try {
                mBus = (MessageBus) Class.forName(mBusType).getConstructor().newInstance();
            } catch (Exception e) {
                LOG.severe(e.getLocalizedMessage());
                return false;
            }
        } else {
            mBus = new SEDABus();
        }
        mBus.start(this.config);

        availableServices = new ArrayList<>();
        registeredServices = new HashMap<>(15);
        runningServices = new HashMap<>(15);
        clients = new HashMap<>(10);

        updateStatus(Status.Running);
        return true;
    }

    @Override
    public boolean pause() {
        return false;
    }

    @Override
    public boolean unpause() {
        return false;
    }

    @Override
    public boolean restart() {
        return false;
    }

    /**
     * Shutdown the Service Bus
     *
     * @return boolean was shutdown successful
     */
    @Override
    public boolean shutdown() {
        updateStatus(Status.Stopping);
        for(final String serviceName : runningServices.keySet()) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    BaseService service = runningServices.get(serviceName);
                    if(service.shutdown()) {
                        runningServices.remove(serviceName);
                    }
                }
            }, serviceName+"-ShutdownThread");
            t.setDaemon(true);
            t.start();
        }
        if(mBus.shutdown()) {
            updateStatus(Status.Stopped);
        } else {
            updateStatus(Status.Errored);
            return false;
        }
        return true;
    }

    /**
     * Ensure teardown is graceful by waiting until all Services indicate graceful teardown complete or timeout
     * @return boolean was graceful shutdown successful
     */
    @Override
    public boolean gracefulShutdown() {
        updateStatus(Status.Stopping);
        List<String> keys = new ArrayList<>(runningServices.keySet());
        for(final String serviceName : keys) {
            AppThread t = new AppThread(new Runnable() {
                @Override
                public void run() {
                    BaseService service = runningServices.get(serviceName);
                    if(service.gracefulShutdown()) {
                        runningServices.remove(serviceName);
                    }
                }
            }, serviceName+"-GracefulShutdownThread");
            t.setDaemon(true);
            t.start();
        }
        boolean allServicesShutdown = false;
        while(!allServicesShutdown) {
            Wait.aSec(1);
            for(String key : keys) {
                if(runningServices.get(key)!=null) {
                    break; // break out of for
                }
            }
            allServicesShutdown = true;
        }
        if(mBus.gracefulShutdown()) {
            updateStatus(Status.Stopped);
        } else {
            updateStatus(Status.Errored);
            return false;
        }
        return true;
    }

    public Status getStatus() {
        return status;
    }
}
