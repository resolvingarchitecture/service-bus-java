package ra.servicebus;

import org.junit.*;
import ra.common.*;
import ra.util.Wait;

import java.util.Properties;
import java.util.logging.Logger;

public class ServiceBusTest {

    private static final Logger LOG = Logger.getLogger(ServiceBusTest.class.getName());

    private static ServiceBus bus;
    private static Properties props;

    @BeforeClass
    public static void init() {
        LOG.info("Init...");
        props = new Properties();
        bus = new ServiceBus();
        bus.start(props);
    }

    @AfterClass
    public static void tearDown() {
        LOG.info("Teardown...");
        bus.gracefulShutdown();
    }

    @Test
    public void verifyPointToPoint() throws ServiceNotAccessibleException, ServiceNotSupportedException, ServiceRegisteredException {
        bus.registerService(MockService.class, props, null);

        Envelope env = Envelope.documentFactory(MockService.id);
        DLC.addRoute(MockService.class.getName(),"Send", env);
        bus.send(env);
        Wait.aSec(2);
    }

}
