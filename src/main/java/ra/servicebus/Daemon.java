package ra.servicebus;

import ra.common.Status;
import ra.common.Config;
import ra.common.Wait;

import java.util.Properties;

public class Daemon {

    private final static Daemon instance = new Daemon();
    private ServiceBus bus;

    public static void main(String[] args) {
        Properties p = Config.loadFromMainArgs(args);
        instance.bus = new ServiceBus(p);
        instance.bus.start(p);
        do {
            Wait.aSec(1);
        } while(instance.bus.getStatus() != Status.Errored
                && instance.bus.getStatus() != Status.Stopped);
    }

}
