package ra.servicebus;

import ra.common.Envelope;
import ra.common.Status;
import ra.common.network.ControlCommand;
import ra.common.service.ServiceNotAccessibleException;
import ra.common.service.ServiceNotSupportedException;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class ClientThread implements Runnable {

    private static final Logger LOG = Logger.getLogger(ClientThread.class.getName());

    private Boolean shutdown = false;
    private Properties config;
    private ServiceBus bus;
    private Socket socket;

    public ClientThread(Properties config, ServiceBus bus, Socket socket) {
        this.config = config;
        this.bus = bus;
        this.socket = socket;
    }

    void shutdown() {
        shutdown = true;
    }

    @Override
    public void run() {
        InputStream input = null;
        OutputStream output = null;
        try {
            input = socket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));

            output = socket.getOutputStream();
            PrintWriter writer = new PrintWriter(output, true);

            while (!shutdown) {
                String json = reader.readLine();
                LOG.info(json);
                Envelope env = Envelope.documentFactory();
                env.fromJSON(json);
                ControlCommand cc = ControlCommand.valueOf(env.getCommandPath());
                switch (cc) {
                    case InitiateComm: {
                        env.addContent("init");
                        writer.write(env.toJSON());
                        break;
                    }
                    case Start: {
                        if(bus.getStatus() == Status.Stopped) {
                            env.addContent(bus.start(config)?"Started":"Start Failed");
                        } else {
                            env.addContent("Bus not Stopped");
                        }
                        writer.write(env.toJSON());
                        break;
                    }
                    case RegisterService: {
                        String interfaceName = (String)env.getValue("interfaceName");
                        String serviceClass = (String)env.getValue("serviceClass");
                        Map<String,String> serviceConfig = (Map<String,String>)env.getValue("serviceConfig");
                        Properties p = new Properties();
                        p.putAll(serviceConfig);
                        boolean serviceRegistered = false;
                        try {
                            if(interfaceName==null)
                                serviceRegistered = bus.registerService(serviceClass, p);
                            else
                                serviceRegistered = bus.registerService(interfaceName, serviceClass, p);
                            env.addContent(serviceRegistered);
                        } catch (ServiceNotAccessibleException e) {
                            env.addContent(e.getClass().getSimpleName());
                        } catch (ServiceNotSupportedException e) {
                            env.addContent(e.getClass().getSimpleName());
                        }
                        writer.write(env.toJSON());
                        break;
                    }
                    case UnregisterService: {

                        break;
                    }
                    case StartService: {
                        String serviceClass = (String)env.getValue("serviceClass");

                        break;
                    }
                    case PauseService: {

                        break;
                    }
                    case UnPauseService: {

                        break;
                    }
                    case RestartService: {

                        break;
                    }
                    case StopService: {

                        break;
                    }
                    case GracefullyStopService: {

                        break;
                    }
                    case Pause: {

                        break;
                    }
                    case UnPause: {

                        break;
                    }
                    case Restart: {

                        break;
                    }
                    case Shutdown: {

                        break;
                    }
                    case GracefullyShutdown: {

                        break;
                    }
                    case CloseClient: {

                        break;
                    }
                    case Ack: {

                        break;
                    }
                    case EndComm: {

                        break;
                    }
                }
            }
        } catch (IOException e) {
            LOG.warning(e.getLocalizedMessage());
        } finally {
            try {
                if(output!=null)
                    output.close();
                if(input!=null)
                    input.close();
                socket.close();
            } catch (IOException e) {}
        }
    }
}
