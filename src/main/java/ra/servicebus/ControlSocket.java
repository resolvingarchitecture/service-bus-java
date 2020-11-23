package ra.servicebus;

import ra.common.Client;
import ra.common.Envelope;
import ra.common.Status;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class ControlSocket implements BusStatusListener, Runnable {

    private static Logger LOG = Logger.getLogger(ControlSocket.class.getName());

    private ServiceBus bus;
    private String ipAddress;
    private Integer port = 0;
    private ServerSocket server;
    private Map<String, Client> clients = new HashMap<>();
    private boolean running = false;
    private boolean shutdown = false;

    ControlSocket(ServiceBus bus) {
        this.bus = bus;
    }
    ControlSocket(ServiceBus bus, int port) {
        this.bus = bus;
        this.port = port;
    }
    ControlSocket(ServiceBus bus, String ipAddress, int port) {
        this.bus = bus;
        this.ipAddress = ipAddress;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            if(ipAddress==null) {
                this.server = new ServerSocket(port, 1, InetAddress.getLocalHost());
            } else {
                this.server = new ServerSocket(port, 1, InetAddress.getByName(ipAddress));
            }
        } catch (IOException e) {
            LOG.severe(e.getLocalizedMessage());
            return;
        }
        LOG.info("Control Socket on " + ipAddress + " listening on " + port);
        running = true;
        while(running) {
            String data = null;
            // Blocking call
            try {
                Socket clientSocket = this.server.accept();
                String clientAddress = clientSocket.getInetAddress().getHostAddress();
                LOG.info("New connection from " + clientAddress);
                Client client = clients.get(clientAddress);
                if(client==null) {
                    // TODO: implement method to return information back to client socket

                    clients.put(clientAddress, client);
                }
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                while ( (data = in.readLine()) != null ) {
                    LOG.info("Message from " + clientAddress + ": " + data);
                }
                Envelope envelope = Envelope.commandFactory();
                envelope.fromJSON(data);
                bus.send(envelope, client);
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
        return this.server.getInetAddress();
    }

    public int getPort() {
        return this.server.getLocalPort();
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
