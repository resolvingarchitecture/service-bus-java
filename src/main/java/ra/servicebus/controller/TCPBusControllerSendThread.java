package ra.servicebus.controller;

import ra.util.Wait;

import java.io.PrintWriter;
import java.util.logging.Logger;

public class TCPBusControllerSendThread implements Runnable {

    private static Logger LOG = Logger.getLogger(TCPBusControllerSendThread.class.getName());

    private boolean running = false;
    private volatile String message;
    private PrintWriter writeToClient;

    public TCPBusControllerSendThread(PrintWriter writeToClient) {
        this.writeToClient = writeToClient;
    }

    public void sendMessage(String message) {
        this.message = message;
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void run() {
        running = true;
        while(running) {
            if(message!=null && !message.isEmpty()) {
                LOG.info("Sending message to client...");
                writeToClient.println(message);
                writeToClient.flush();
                message = null;
            }
            Wait.aMs(100);
        }
    }
}
