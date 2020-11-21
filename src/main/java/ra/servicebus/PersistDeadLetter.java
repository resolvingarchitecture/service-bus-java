package ra.servicebus;

import ra.common.Envelope;
import ra.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;

/**
 * TODO: Needs completed
 */
public class PersistDeadLetter implements Runnable {

    private static final Logger LOG = Logger.getLogger(PersistDeadLetter.class.getName());

    private static final Integer MAX_DEADLETTER_FILE_SIZE = 10;
    private static final Integer MAX_DEADLETTER_FILES = 3;

    private Envelope envelope;
    private String path;

    public PersistDeadLetter(Envelope envelope, String path) {
        this.envelope = envelope;
        this.path = path;
    }

    @Override
    public void run() {
        try {
            FileUtil.appendFile(envelope.toJSON().getBytes(), path);
        } catch (IOException e) {
            LOG.warning(e.getLocalizedMessage());
        }
    }
}
