package ra.servicebus;

import ra.common.service.ServiceStatus;

/**
 * Notified whenever a service on the bus reports a new {@link ServiceStatus}.
 * Register with {@link ServiceBus#registerServiceStatusListener}.
 */
public interface ServiceStatusListener {
    void serviceStatusChanged(String serviceFullName, ServiceStatus serviceStatus);
}
