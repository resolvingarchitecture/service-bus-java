package ra.servicebus.controller;

import ra.common.Client;
import ra.common.Envelope;
import ra.common.Status;
import ra.common.network.ControlCommand;
import ra.common.notification.SubscriptionRequest;
import ra.common.service.ServiceNotAccessibleException;
import ra.common.service.ServiceNotSupportedException;
import ra.servicebus.ServiceBus;
import ra.util.Wait;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class TCPBusControllerReceiveThread implements Client, Runnable {

    private static Logger LOG = Logger.getLogger(TCPBusControllerReceiveThread.class.getName());

    private ServiceBus bus;
    private Properties config;
    private TCPBusController tcpBusController;
    private String socketAddress;
    private BufferedReader readFromClient = null;
    private boolean running = false;

    public TCPBusControllerReceiveThread(ServiceBus bus, Properties config, TCPBusController tcpBusController, String socketAddress, BufferedReader readFromClient) {
        this.bus = bus;
        this.config = config;
        this.tcpBusController = tcpBusController;
        this.socketAddress = socketAddress;
        this.readFromClient = readFromClient;
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void reply(Envelope envelope) {
        tcpBusController.sendMessage(envelope);
    }

    public void run() {
        try {
            running = true;
            while(running) {
                // Wait for client to send data
                String json = readFromClient.readLine();
                if(json==null || json.isEmpty()) {
                    LOG.info("No data in stream; client likely closed so close down connection.");
                    tcpBusController.shutdown(socketAddress);
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
                            tcpBusController.clientSocketAddresses.put(clientIdStr, socketAddress);
                            env.addNVP("init","true");
                        }
                        tcpBusController.sendMessage(env);
                        break;
                    }
                    case Send: {
                        bus.send(env, this);
                        break;
                    }
                    case RegisterService: {
                        env.addContent("RegisterService not supported at this time.");
                        String interfaceName = (String)env.getValue("interfaceName");
                        String serviceClass = (String)env.getValue("serviceClass");
                        Map<String,String> serviceConfig = (Map<String,String>)env.getValue("serviceConfig");
                        Properties p = new Properties();
                        if(serviceConfig!=null) {
                            p.putAll(serviceConfig);
                        }
                        boolean serviceRegistered = false;
                        try {
                            if(interfaceName==null) {
                                serviceRegistered = bus.registerService(serviceClass, p);
                            } else {
                                serviceRegistered = bus.registerService(interfaceName, serviceClass, p);
                            }
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
                        env.addContent("UnregisterService not supported at this time.");
                        break;
                    }
                    case StartService: {
                        env.addContent("StartService not supported at this time.");
                        String serviceClass = (String)env.getValue("serviceClass");
                        boolean serviceStarted = bus.startService(serviceClass);
                        env.addContent(serviceStarted);
                        break;
                    }
                    case PauseService: {
                        env.addContent("PauseService not supported at this time.");
                        break;
                    }
                    case UnPauseService: {
                        env.addContent("UnPauseService not supported at this time.");
                        break;
                    }
                    case RestartService: {
                        env.addContent("RestartService not supported at this time.");
                        break;
                    }
                    case StopService: {
                        env.addContent("StopService not supported at this time.");
                        break;
                    }
                    case GracefullyStopService: {
                        env.addContent("GracefullyStopService not supported at this time.");
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
                    case Pause: {
                        env.addContent("Pause not supported at this time.");
                        break;
                    }
                    case UnPause: {
                        env.addContent("UnPause not supported at this time.");
                        break;
                    }
                    case Restart: {
                        env.addContent("Restart not supported at this time.");
                        if(tcpBusController.clients.size() <= 1) {
                            // Ensure only one client using this bus

                        }
                        break;
                    }
                    case Shutdown: {
                        env.addContent("Shutdown not supported at this time.");
                        if(tcpBusController.clients.size() <= 1) {
                            // Ensure only one client using this bus

                        }
                        break;
                    }
                    case GracefullyShutdown: {
                        env.addContent("GracefullyShutdown not supported at this time.");
                        break;
                    }
                    case CloseClient: {
                        // Client is letting bus controller know it's closing so close socket
                        tcpBusController.shutdown(tcpBusController.clientSocketAddresses.get(env.getClient()));
                        break;
                    }
                    case Ack: {
                        env.addContent("Ack not supported at this time.");
                        break;
                    }
                    case EndComm: {
                        env.addContent("EndComm not supported at this time.");
                        break;
                    }
                }
            }
        } catch(Exception exp) {
            LOG.warning(exp.getLocalizedMessage());
        }
    }
}
