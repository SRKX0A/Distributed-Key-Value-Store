package app_kvClient;

import java.io.*;
import java.net.*;

import org.apache.log4j.Logger;

public class ClientSubscriptionServer extends Thread {

    private static Logger logger = Logger.getRootLogger();

    private ServerSocket serverSocket;
    private volatile boolean online;

    public ClientSubscriptionServer(String address, int port) throws IOException {
	this.serverSocket = new ServerSocket(port, 0, InetAddress.getByName(address));
	this.online = true;
    }

    public void run() {

	while (this.online) {
            try {
                Socket server = this.serverSocket.accept();
                new SubscriptionConnection(server).start();
                logger.info(String.format("Connected to %s on port %d", server.getInetAddress().getHostName(), server.getPort()));
            } catch (SocketException e) {
                logger.info(String.format("SocketException received: %s", e.toString()));
            } catch (IOException e) {
                logger.error(String.format("Unable to establish connection: %s", e.toString()));
            }
	}

    }

    public void close() {
	
	this.online = false;

	try {
	    if (this.serverSocket != null) {
		this.serverSocket.close();
	    }
	} catch (IOException ioe) {
	    logger.error("Failed to gracefully close connection: " + ioe.toString());
	}

    }

}
