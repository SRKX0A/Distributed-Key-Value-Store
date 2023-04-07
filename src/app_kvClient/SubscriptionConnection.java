package app_kvClient;

import java.io.*;
import java.net.*;

import org.apache.log4j.Logger;

import shared.messages.SubscriptionMessage;
import shared.messages.SubscriptionMessage.StatusType;

public class SubscriptionConnection extends Thread {

    private static Logger logger = Logger.getRootLogger();

    private Socket socket;

    private ObjectInputStream input;
    private ObjectOutputStream output;

    public SubscriptionConnection(Socket socket) throws Exception {
	this.socket = socket;
	this.output = new ObjectOutputStream(this.socket.getOutputStream());
	this.input = new ObjectInputStream(this.socket.getInputStream());
    }

    public void run() {

	try {

	    SubscriptionMessage request = receiveMessage(this.input);

	    if (request.getStatus() == StatusType.KV_NOTIFICATION) {
		this.handleKVNotificationMessage(request);
	    }

	} catch (EOFException eofe) {
	    this.handleEOFException(eofe);
	} catch(Exception e) {
	    this.handleGeneralException(e);
	}

    }

    public void handleKVNotificationMessage(SubscriptionMessage request) {
	System.out.println(String.format("%s: key = %s, old value = %s, new value = %s", request.getStatus(), request.getKey(), request.getOldValue(), request.getNewValue()));
    }

    public void handleEOFException(EOFException eofe) {
	logger.info("Client connection closed: " + eofe.toString());
    }

    public void handleGeneralException(Exception e) {
	logger.info("Client connection failure: " + e.toString());

	try {
	    this.output.close();
	    this.input.close();
	    this.socket.close();
	} catch (IOException ioe) {
	    logger.error("Failed to gracefully close connection: " + ioe.toString());
	}
    }

    public static ServerMessage receiveMessage(ObjectInputStream input) throws Exception {

	ServerMessage request = (SubscriptionMessage) input.readObject();

	logger.debug(String.format("Client subscription server received request from server with status = %s", request.getStatus()));

	return request;
    }

}
