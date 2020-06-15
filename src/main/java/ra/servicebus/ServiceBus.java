package ra.servicebus;


import ra.common.*;
import ra.sedabus.SEDABus;
import ra.util.AppThread;
import ra.util.Config;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 */
public final class ServiceBus implements MessageProducer, LifeCycle, ServiceRegistrar, ServiceStatusListener {

    private static final Logger LOG = Logger.getLogger(ServiceBus.class.getName());

    private Status status = Status.Stopped;

    private Properties properties;

    private MessageBus mBus;

    private Map<String, BaseService> registeredServices;
    private Map<String, BaseService> runningServices;

    private List<BusStatusListener> busStatusListeners = new ArrayList<>();

    public ServiceBus() {}

    @Override
    public boolean send(Envelope e) {
        LOG.info("Received envelope. Publishing to channel...");
        return mBus.publish(e);
    }

    public void registerBusStatusListener (BusStatusListener busStatusListener) {
        busStatusListeners.add(busStatusListener);
    }

    public void unregisterBusStatusListener(BusStatusListener busStatusListener) {
        busStatusListeners.remove(busStatusListener);
    }

    public boolean registerService(Class serviceClass, Properties p, List<ServiceStatusObserver> observers) throws ServiceNotAccessibleException, ServiceNotSupportedException, ServiceRegisteredException {
        LOG.info("Registering service class: "+serviceClass.getName());
        if(registeredServices.containsKey(serviceClass.getName())) {
            throw new ServiceRegisteredException();
        }
        if(p != null && p.size() > 0)
            properties.putAll(p);
        final String serviceName = serviceClass.getName();
        try {
            final BaseService service = (BaseService)Class.forName(serviceName).getConstructor().newInstance();
            service.setProducer(this);
            mBus.registerChannel(serviceName);
            mBus.registerAsynchConsumer(serviceName, service);
            // register service
            registeredServices.put(serviceName, service);
            service.registerServiceStatusListener(this);
            service.setRegistered(true);
            if(observers != null) {
                LOG.info("Registering ServiceStatusObservers with service: "+service.getClass().getName());
                registerServiceStatusObservers(serviceClass, observers);
            }
            LOG.info("Service registered successfully: "+serviceName);
            // init registered service
            new AppThread(new Runnable() {
                @Override
                public void run() {
                    if(service.start(properties)) {
                        runningServices.put(serviceName, service);
                        LOG.info("Service registered successfully as running: "+serviceName);
                    } else {
                        LOG.warning("Registered service failed to start: "+serviceName);
                    }
                }
            }, serviceName+"-StartupThread").start();
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

    public boolean unregisterService(Class serviceClass) {
        if(runningServices.containsKey(serviceClass.getName())) {
            final String serviceName = serviceClass.getName();
            final BaseService service = runningServices.get(serviceName);
            new AppThread(new Runnable() {
                @Override
                public void run() {
                    if(service.shutdown()) {
                        runningServices.remove(serviceName);
                        registeredServices.remove(serviceName);
                        service.setRegistered(false);
                        LOG.info("Service unregistered successfully: "+serviceName);
                    }
                }
            }, serviceName+"-ShutdownThread").start();
        }
        return true;
    }

    public void registerServiceStatusObservers(Class serviceClass, List<ServiceStatusObserver> observers) {
        if(registeredServices.containsKey(serviceClass.getName())) {
            BaseService service = registeredServices.get(serviceClass.getName());
            LOG.info("Registering ServiceStatusObservers with service: "+service.getClass().getName());
            service.registerServiceStatusObservers(observers);
        }
    }

    public void unregisterServiceStatusObservers(Class serviceClass, ServiceStatusObserver observer) {
        if(registeredServices.containsKey(serviceClass.getName())) {
            BaseService service = registeredServices.get(serviceClass.getName());
            LOG.info("Unregistering ServiceStatusObserver with service: "+service.getClass().getName());
            service.unregisterServiceStatusObserver(observer);
        }
    }

    public List<ServiceReport> serviceReports(){
        List<ServiceReport> serviceReports = new ArrayList<>(registeredServices.size());
        ServiceReport r;
        for(BaseService s : registeredServices.values()) {
            r = new ServiceReport();
            r.registered = true;
            r.running = runningServices.containsKey(s.getClass().getName());
            r.serviceClassName = s.getClass().getName();
            r.serviceStatus = s.getServiceStatus();
            serviceReports.add(r);
        }
        return serviceReports;
    }

    private void updateStatus(Status status) {
        this.status = status;
        switch(status) {
            case Starting: {
                LOG.info("1M5 Service Bus is Starting");
                break;
            }
            case Running: {
                LOG.info("1M5 Service Bus is Running");
                break;
            }
            case Stopping: {
                LOG.info("1M5 Service Bus is Stopping");
                break;
            }
            case Stopped: {
                LOG.info("1M5 Service Bus has Stopped");
                break;
            }
        }
        LOG.info("Updating Bus Status Listeners; size="+busStatusListeners.size());
        for(BusStatusListener l : busStatusListeners) {
            l.busStatusChanged(status);
        }
    }

    @Override
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
                if(allServicesWithStatus(ServiceStatus.RUNNING)) {
                    LOG.info("All Services are RUNNING therefore Bus updating status to RUNNING.");
                    updateStatus(Status.Running);
                }
                break;
            }
            case SHUTDOWN: {
                if(allServicesWithStatus(ServiceStatus.SHUTDOWN)) {
                    LOG.info("All Services are SHUTDOWN therefore Bus updating status to STOPPED.");
                    updateStatus(Status.Stopped);
                }
                break;
            }
            case GRACEFULLY_SHUTDOWN: {
                if(allServicesWithStatus(ServiceStatus.GRACEFULLY_SHUTDOWN)) {
                    LOG.info("All Services are GRACEFULLY_SHUTDOWN therefore Bus updating status to STOPPED.");
                    updateStatus(Status.Stopped);
                }
                break;
            }
        }
    }

    private Boolean allServicesWithStatus(ServiceStatus serviceStatus) {
        Collection<BaseService> services = registeredServices.values();
        for(BaseService s : services) {
            if(s.getServiceStatus() != serviceStatus){
                return false;
            }
        }
        return true;
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
            this.properties = Config.loadFromClasspath("ra-servicebus.config", properties, false);
        } catch (Exception e) {
            LOG.warning(e.getLocalizedMessage());
            this.properties = properties;
        }
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
        mBus.start(this.properties);

        registeredServices = new HashMap<>(15);
        runningServices = new HashMap<>(15);

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
     * TODO: Run in separate AppThread
     *
     * @return boolean was shutdown successful
     */
    @Override
    public boolean shutdown() {
        updateStatus(Status.Stopping);
        for(final String serviceName : runningServices.keySet()) {
            new AppThread(new Runnable() {
                @Override
                public void run() {
                    BaseService service = runningServices.get(serviceName);
                    if(service.shutdown()) {
                        runningServices.remove(serviceName);
                    }
                }
            }, serviceName+"-ShutdownThread").start();
        }
        return mBus.shutdown();
    }

    /**
     * Ensure teardown is graceful by waiting until all Services indicate graceful teardown complete or timeout
     * @return boolean was graceful shutdown successful
     */
    @Override
    public boolean gracefulShutdown() {
        updateStatus(Status.Stopping);
        for(final String serviceName : runningServices.keySet()) {
            Thread t = new Thread(new Runnable() {
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
        return mBus.gracefulShutdown();
    }

    public Status getStatus() {
        return status;
    }
}
