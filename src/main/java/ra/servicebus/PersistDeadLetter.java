package ra.servicebus;

import ra.common.Envelope;
import ra.common.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Appends a dead-lettered {@link Envelope} (as JSON, one per line) to the bus
 * dead-letter file, rotating it when it grows past {@link #MAX_FILE_BYTES} and
 * keeping at most {@link #MAX_FILES} rotations.
 */
public class PersistDeadLetter implements Runnable {

    private static final Logger LOG = Logger.getLogger(PersistDeadLetter.class.getName());

    /** Rotate once the active file passes this size. */
    static final long MAX_FILE_BYTES = 5L * 1024 * 1024;
    /** Keep this many rotated files (deadLetter.json.1 .. .N); older ones are deleted. */
    static final int MAX_FILES = 5;

    private final Envelope envelope;
    private final String path;

    public PersistDeadLetter(Envelope envelope, String path) {
        this.envelope = envelope;
        this.path = path;
    }

    @Override
    public void run() {
        if (path == null) {
            LOG.warning("no dead-letter path; dropping " + envelope.getId());
            return;
        }
        try {
            rotateIfNeeded();
            FileUtil.appendFile((envelope.toJSON() + System.lineSeparator()).getBytes(), path);
        } catch (IOException e) {
            LOG.warning("dead-letter write failed for " + envelope.getId() + ": " + e.getMessage());
        }
    }

    private synchronized void rotateIfNeeded() {
        File active = new File(path);
        if (!active.exists() || active.length() < MAX_FILE_BYTES) return;

        File oldest = new File(path + "." + MAX_FILES);
        if (oldest.exists() && !oldest.delete()) {
            LOG.warning("could not delete " + oldest);
        }
        for (int i = MAX_FILES - 1; i >= 1; i--) {
            File from = new File(path + "." + i);
            if (from.exists() && !from.renameTo(new File(path + "." + (i + 1)))) {
                LOG.warning("could not rotate " + from);
            }
        }
        if (!active.renameTo(new File(path + ".1"))) {
            LOG.warning("could not rotate active dead-letter file");
        }
    }
}
