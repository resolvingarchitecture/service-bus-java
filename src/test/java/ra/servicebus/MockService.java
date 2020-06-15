package ra.servicebus;

import org.junit.Assert;
import ra.common.BaseService;
import ra.common.Envelope;

public class MockService extends BaseService {

    public static final int id = 10;

    @Override
    public void handleDocument(Envelope envelope) {
        Assert.assertTrue(envelope.getId() == id);
    }
}
