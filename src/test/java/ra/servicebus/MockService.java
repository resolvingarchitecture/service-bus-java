package ra.servicebus;

import org.junit.Assert;
import ra.common.Envelope;
import ra.common.service.BaseService;

public class MockService extends BaseService {

    public static final String id = "10";

    @Override
    public void handleDocument(Envelope envelope) {
        Assert.assertTrue(envelope.getId().equals(id));
    }
}
