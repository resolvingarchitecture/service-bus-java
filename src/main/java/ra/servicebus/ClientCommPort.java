package ra.servicebus;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;

public class ClientCommPort {
    public UUID id;
    public Integer clientSocketPort;
    public Socket clientSocket;
    public Integer serverSocketPort;
    public ServerSocket serverSocket;
}
