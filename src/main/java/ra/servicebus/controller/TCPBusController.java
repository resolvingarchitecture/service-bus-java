package ra.servicebus.controller;

import ra.common.Envelope;
import ra.servicebus.ServiceBus;
import ra.util.Wait;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

public class TCPBusController implements Runnable {

    private static final Logger LOG = Logger.getLogger(TCPBusController.class.getName());

    final UUID id;
    final ServiceBus bus;
    final Properties config;
    private final Integer port;

    private ServerSocket serverSocket;
    private Socket socket;
    private BufferedReader readFromClient;
    private TCPBusControllerReceiveThread receiveThread;
    private PrintWriter writeToClient;
    private TCPBusControllerSendThread sendThread;
    private boolean running = false;

    public TCPBusController(ServiceBus bus, Properties config, Integer port) {
        id = UUID.randomUUID();
        this.bus = bus;
        this.config = config;
        this.port = port;
    }

    public boolean isRunning() {
        return running;
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            LOG.info("Waiting for connection...");
            socket = serverSocket.accept();
            LOG.info("Connection accepted...");
            readFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writeToClient = new PrintWriter(socket.getOutputStream(), true);
            receiveThread = new TCPBusControllerReceiveThread(bus, config, this, readFromClient);
            sendThread = new TCPBusControllerSendThread(writeToClient);
            Thread receive = new Thread(receiveThread);
            Thread send = new Thread(sendThread);
            receive.start();
            send.start();
            running = true;
            while(running) {
                Wait.aMs(100);
            }
        } catch (IOException e) {
            LOG.severe(e.getLocalizedMessage());
        }
    }

    public void sendMessage(Envelope message) {
        sendThread.sendMessage(message.toJSONRaw());
    }
}
