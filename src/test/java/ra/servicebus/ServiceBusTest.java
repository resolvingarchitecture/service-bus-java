package ra.servicebus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ra.common.Envelope;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ServiceBusTest {

    private static final Logger LOG = Logger.getLogger(ServiceBusTest.class.getName());

    private ServiceBus bus;
    private Properties props;

    @Before
    public void init() {
        MockService.reset();
        props = new Properties();
        bus = new ServiceBus(props);
        Assert.assertTrue(bus.start(props));
    }

    @After
    public void tearDown() {
        bus.gracefulShutdown();
    }

    @Test
    public void registerAndStart_thenDiscover() {
        Assert.assertTrue(bus.registerAndStartService(MockService.class));
        Assert.assertTrue("service should reach running",
                bus.awaitRunning(5000, MockService.class));

        Assert.assertTrue(bus.isRegistered(MockService.class));
        Assert.assertTrue(bus.isRunning(MockService.class));
        Assert.assertNotNull(bus.getService(MockService.class));
        Assert.assertEquals(1, bus.findRunningServices(MockService.class).size());
        Assert.assertEquals(1, bus.findRunningServices(ra.common.service.Service.class).size());
        Assert.assertTrue(MockService.STARTED);
    }

    @Test
    public void routesEnvelopeToServiceAndFiresCallback() throws Exception {
        bus.registerAndStartService(MockService.class);
        bus.awaitRunning(5000, MockService.class);

        CountDownLatch replied = new CountDownLatch(1);
        Envelope e = Envelope.documentFactory();
        e.addRoute(MockService.class, "HANDLE");
        e.ratchet();
        bus.send(e, envelope -> replied.countDown());

        Assert.assertTrue(replied.await(5, TimeUnit.SECONDS));
        Assert.assertTrue(MockService.RECEIVED.contains(e.getId()));
    }

    @Test
    public void pauseAndUnpause() {
        bus.registerAndStartService(MockService.class);
        bus.awaitRunning(5000, MockService.class);
        Assert.assertTrue(bus.pause());
        Assert.assertEquals(ra.common.Status.Paused, bus.getStatus());
        Assert.assertTrue(bus.unpause());
        Assert.assertEquals(ra.common.Status.Running, bus.getStatus());
    }
}
