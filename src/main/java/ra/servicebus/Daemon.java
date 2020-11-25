package ra.servicebus;

import ra.common.Status;
import ra.util.Config;
import ra.util.Wait;

public class Daemon implements BusStatusListener {

    private final static Daemon instance = new Daemon();
    private ServiceBus bus;
    private Status busStatus = Status.Starting;

    public static void main(String[] args) {
        instance.bus = new ServiceBus(Config.loadFromMainArgs(args));
        instance.bus.registerBusStatusListener(instance);
        new Thread(instance.bus).start();
        do {
            Wait.aSec(1);
        } while(instance.busStatus != Status.Errored
                && instance.busStatus != Status.Stopped);
    }

    @Override
    public void busStatusChanged(Status status) {
        busStatus = status;
    }
}
