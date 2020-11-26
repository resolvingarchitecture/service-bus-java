package ra.servicebus;

import ra.common.*;
import ra.common.network.TCPClient;
import ra.util.RandomUtil;
import ra.util.Wait;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.logging.Logger;

/**
 * Sets up a Client Socket and Server Socket for accepting
 * commands from Clients, sending Envelopes to Services,
 * and sending Notifications to Clients.
 */
public class TCPBusController implements BusStatusListener, Runnable {

    private static Logger LOG = Logger.getLogger(TCPBusController.class.getName());

    public static Integer HANDSHAKE_PORT = 2013;

    private Properties config;
    private ServiceBus bus;
    // Server Socket for requesting a port for further communications
    private ServerSocket serverSocket;
    private List<ClientThread> clients = new ArrayList<>();
    private boolean running = false;
    private boolean shutdown = false;

    public TCPBusController(Properties config, ServiceBus bus) {
        this.config = config;
        this.bus = bus;
    }

    @Override
    public void run() {
        try {
            this.serverSocket = new ServerSocket(HANDSHAKE_PORT);
        } catch (IOException e) {
            LOG.severe(e.getLocalizedMessage());
            return;
        }
        LOG.info("BusTCPController: Handshake Socket on localhost listening on "+HANDSHAKE_PORT);
        running = true;
        while(running) {
            try {
                Socket clientSocket = this.serverSocket.accept();
                String clientAddress = clientSocket.getInetAddress().getHostAddress();
                if(!("localhost".equals(clientAddress)) && !("127.0.0.1".equals(clientAddress))) {
                    LOG.warning("Unable to accept client command control requests from addresses other than localhost.");
                    continue;
                }
                ClientThread clientThread = new ClientThread(config, bus, clientSocket);
                clients.add(clientThread);
                new Thread(clientThread).start();
            } catch (IOException e) {
                LOG.severe(e.getLocalizedMessage());
                Wait.aSec(1000);
            }
            if(shutdown) {
                running = false;
            }
        }
    }

    public InetAddress getSocketAddress() {
        return this.serverSocket.getInetAddress();
    }

    public int getPort() {
        return this.serverSocket.getLocalPort();
    }

    public void shutdown() {
        shutdown = true;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void busStatusChanged(Status status) {

    }
}
