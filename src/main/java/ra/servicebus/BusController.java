package ra.servicebus;

import ra.common.*;
import ra.util.RandomUtil;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Sets up a Client Socket and Server Socket for accepting
 * commands from Clients, sending Envelopes to Services,
 * and sending Notifications to Clients.
 */
public class BusController implements BusStatusListener, Runnable {

    private static Logger LOG = Logger.getLogger(BusController.class.getName());

    private ServiceBus bus;
    // Server Socket for requesting a port for further communications
    private ServerSocket serverHandshakeSocket;
    private Map<String, ClientCommPort> commPorts = new HashMap<>();
    private boolean running = false;
    private boolean shutdown = false;

    BusController(ServiceBus bus) {
        this.bus = bus;
    }

    @Override
    public void run() {
        try {
            this.serverHandshakeSocket = new ServerSocket(2013, 1, InetAddress.getLocalHost());
        } catch (IOException e) {
            LOG.severe(e.getLocalizedMessage());
            return;
        }
        LOG.info("Control Socket on localhost listening on 2013");
        running = true;
        while(running) {
            // Blocking call
            try {
                Socket clientSocket = this.serverHandshakeSocket.accept();
                String clientAddress = clientSocket.getInetAddress().getHostAddress();
                LOG.info("New connection from " + clientAddress);
                if(!("localhost".equals(clientAddress)) && !("127.0.0.1".equals(clientAddress))) {
                    LOG.warning("Unable to accept client command control requests from addresses other than localhost.");
                    continue;
                }
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String data = in.readLine();
                int command = Integer.parseInt(data);
                if(ControlCommand.InitiateComm.ordinal() == command) {
                    ClientCommPort ccPort = new ClientCommPort();
                    ccPort.id = UUID.randomUUID();
                    ccPort.serverSocketPort = RandomUtil.nextRandomInteger(10000, 0xFFFF);
                    ccPort.serverSocket = new ServerSocket(ccPort.serverSocketPort, 1, InetAddress.getLocalHost());
                    ccPort.clientSocketPort = RandomUtil.nextRandomInteger(10000, 0xFFFF);
                    if(ccPort.clientSocketPort.equals(ccPort.serverSocketPort)) {
                        ccPort.clientSocketPort = RandomUtil.nextRandomInteger(10000, 0xFFFF);
                    }
                    ccPort.clientSocket = new Socket();
                } else {
                    LOG.warning("Only InitiateComm supported on port 2013.");
                }
            } catch (IOException e) {
                LOG.severe(e.getLocalizedMessage());
                return;
            }
            if(shutdown) {
                running = false;
            }
        }
    }

    public InetAddress getSocketAddress() {
        return this.serverHandshakeSocket.getInetAddress();
    }

    public int getPort() {
        return this.serverHandshakeSocket.getLocalPort();
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
