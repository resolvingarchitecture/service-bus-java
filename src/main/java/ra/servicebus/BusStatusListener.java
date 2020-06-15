package ra.servicebus;

import ra.common.Status;

/**
 * Updates observers interested in ServiceBus status changes.
 */
public interface BusStatusListener {
    void busStatusChanged(Status status);
}
