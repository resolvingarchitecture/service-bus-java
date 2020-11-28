package ra.servicebus.controller;

import ra.common.Envelope;
import ra.common.Status;
import ra.common.network.ControlCommand;
import ra.common.service.ServiceNotAccessibleException;
import ra.common.service.ServiceNotSupportedException;
import ra.servicebus.ServiceBus;
import ra.util.Wait;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

public class TCPBusControllerReceiveThread implements Runnable {

    private static Logger LOG = Logger.getLogger(TCPBusControllerReceiveThread.class.getName());

    private ServiceBus bus;
    private Properties config;
    private TCPBusController tcpBusController;
    private BufferedReader readFromClient = null;
    private boolean running = false;
    private UUID client;

    public TCPBusControllerReceiveThread(ServiceBus bus, Properties config, TCPBusController tcpBusController, BufferedReader readFromClient) {
        this.bus = bus;
        this.config = config;
        this.tcpBusController = tcpBusController;
        this.readFromClient = readFromClient;
    }

    public void run() {
        try {
            running = true;
            while(running) {
                String json = readFromClient.readLine();
                if(json==null || json.isEmpty()) {
                    LOG.info("No data in stream - wait a sec");
                    Wait.aSec(1);
                    continue;
                } else {
                    LOG.info(json);
                }
                Envelope env = Envelope.documentFactory();
                env.fromJSON(json);
                ControlCommand cc = ControlCommand.valueOf(env.getCommandPath());
                LOG.info("ControlCommand: "+env.getCommandPath());
                switch (cc) {
                    case InitiateComm: {
                        String clientIdStr = env.getClient();
                        if(clientIdStr==null) {
                            env.addErrorMessage("No Client Id");
                        } else {
                            client = UUID.fromString(env.getClient());
                            env.addNVP("init","true");
                        }
                        tcpBusController.sendMessage(env);
                        break;
                    }
                    case Start: {
                        if(bus.getStatus() == Status.Stopped) {
                            env.addContent(bus.start(config)?"Started":"Start Failed");
                        } else {
                            env.addContent("Bus not Stopped");
                        }
                        tcpBusController.sendMessage(env);
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
                        tcpBusController.sendMessage(env);
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
                        String senderId = env.getClient();
                        if(tcpBusController.id.toString().equals(senderId)) {
                            LOG.info("Sent Ack returned Acknowledged.");
                        } else {
                            LOG.info("Client requesting ack; returning...");
                            tcpBusController.sendMessage(env);
                        }
                        break;
                    }
                    case EndComm: {

                        break;
                    }
                }
            }
        } catch(Exception exp) {
            exp.printStackTrace();
        }
    }
}
