package ra.servicebus.controller;

import ra.common.Envelope;
import ra.common.Tuple3;
import ra.common.network.ControlCommand;
import ra.servicebus.ServiceBus;
import ra.util.Wait;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Enables Control of Bus over TCP Socket
 * TODO: Enable control from multiple clients
 */
public class TCPBusController implements Runnable {

    private static final Logger LOG = Logger.getLogger(TCPBusController.class.getName());

    private static final Integer MAX_CLIENTS = 30;

    final String id;
    // Client Socket Address, Socket, Receive Thread, Send Thread
    final Map<String, Tuple3<Socket,TCPBusControllerReceiveThread, TCPBusControllerSendThread>> clients = new HashMap<>();
    // Client Id, Client Socket Address
    final Map<String, String> clientSocketAddresses = new HashMap<>();
    final ServiceBus bus;
    final Properties config;
    private final Integer port;

    private ServerSocket serverSocket;
//    private Socket socket;
//    private TCPBusControllerReceiveThread receiveThread;
//    private TCPBusControllerSendThread sendThread;
    private boolean running = false;

    public TCPBusController(ServiceBus bus, Properties config, Integer port) {
        id = UUID.randomUUID().toString();
        this.bus = bus;
        this.config = config;
        this.port = port;
    }

    public boolean isRunning() {
        return running;
    }

    public void endOfRoute(Envelope envelope) {
        sendMessage(envelope);
    }

    public void shutdown(String clientSocketAddress) {
        Envelope env = Envelope.documentFactory();
        env.setCommandPath(ControlCommand.CloseClient.name());
        String json = env.toJSONRaw();
        Tuple3<Socket,TCPBusControllerReceiveThread, TCPBusControllerSendThread> t = clients.get(clientSocketAddress);
        t.second.shutdown();
        t.third.sendMessage(json);
        t.third.shutdown();
        try {
            t.first.close();
        } catch (IOException e) {
            LOG.info(e.getLocalizedMessage());
        }
    }

    public void shutdown() {
//        if(receiveThread!=null) receiveThread.shutdown();
//        if(sendThread!=null) sendThread.shutdown();
        for(String clientSocketAddress : clientSocketAddresses.values()) {
            shutdown(clientSocketAddress);
        }
        running = false;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            running = true;
            while(running) {
                if(clients.size() > MAX_CLIENTS) {
                    // Wait until a client disconnects to offer a new connection limiting number of threads used to MAX_CLIENTS * 2 + 1
                    Wait.aSec(1);
                    continue;
                }
                LOG.info("Waiting for new connection...");
                Socket socket = serverSocket.accept();
                String socketAddress = socket.getRemoteSocketAddress().toString();
                LOG.info("Connection accepted from: "+socketAddress);
                TCPBusControllerReceiveThread receiveThread = new TCPBusControllerReceiveThread(
                        bus,
                        config,
                        this,
                        socketAddress,
                        new BufferedReader(new InputStreamReader(socket.getInputStream())));
                TCPBusControllerSendThread sendThread = new TCPBusControllerSendThread(new PrintWriter(socket.getOutputStream(), true));
                Thread receive = new Thread(receiveThread);
                Thread send = new Thread(sendThread);
                receive.start();
                send.start();
                Tuple3<Socket,TCPBusControllerReceiveThread, TCPBusControllerSendThread> t = new Tuple3<>(socket, receiveThread, sendThread);
                clients.put(socket.getRemoteSocketAddress().toString(), t);
            }
        } catch (IOException e) {
            LOG.severe(e.getLocalizedMessage());
        }
    }

    public void sendMessage(Envelope message) {
        if(message.getClient()==null) {
            // Not meant to be sent to client
            LOG.fine("Not meant to be sent to client - ignoring; envelope.id: "+message.getId());
            return;
        }
        String socketAddress = clientSocketAddresses.get(message.getClient());
        if(socketAddress==null) {
            // Client no longer around, log it
            LOG.warning("Client no longer around to send message; \n\tenvelope.clientId: "+message.getClient()+" \n\tenvelope.id: "+message.getId());
            return;
        }
        Tuple3<Socket,TCPBusControllerReceiveThread, TCPBusControllerSendThread> t = clients.get(socketAddress);
        message.setClient(id); // Set client to this server socket
        t.third.sendMessage(message.toJSONRaw());
    }
}
