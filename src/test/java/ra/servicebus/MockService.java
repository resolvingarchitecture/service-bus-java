package ra.servicebus;

import ra.common.Envelope;
import ra.common.service.BaseService;

import java.util.concurrent.ConcurrentLinkedQueue;

/** Test service. The bus instantiates it reflectively, so it reports via statics. */
public class MockService extends BaseService {

    public static final ConcurrentLinkedQueue<String> RECEIVED = new ConcurrentLinkedQueue<>();
    public static volatile boolean STARTED = false;

    public static void reset() {
        RECEIVED.clear();
        STARTED = false;
    }

    @Override
    public boolean start(java.util.Properties p) {
        boolean ok = super.start(p);
        STARTED = true;
        return ok;
    }

    @Override
    public void handleDocument(Envelope envelope) {
        RECEIVED.add(envelope.getId());
    }

    @Override
    public void handleHeaders(Envelope envelope) {
        RECEIVED.add(envelope.getId());
    }
}
