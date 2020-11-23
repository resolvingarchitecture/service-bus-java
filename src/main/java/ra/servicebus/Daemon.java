package ra.servicebus;

import ra.common.Status;
import ra.util.Config;
import ra.util.Wait;

public class Daemon {

    private static ServiceBus bus = new ServiceBus();

    public static void main(String[] args) {
        bus.start(Config.loadFromMainArgs(args));
        do {
            Wait.aSec(60);
        } while(bus.getStatus() != Status.Errored
                && bus.getStatus() != Status.Stopped);
    }

}
