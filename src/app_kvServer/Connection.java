package app_kvServer;

import java.io.*;
import java.net.*;

import org.apache.log4j.Logger;

import client.ProtocolMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class Connection extends Thread {
    
    private static Logger logger = Logger.getRootLogger();
    
    private KVServer kvServer;

    private Socket socket;

    private InputStream input;
    private OutputStream output;

    private boolean isOpen;

    public Connection(Socket socket, KVServer kvServer) throws IOException {
	this.kvServer = kvServer;

	this.socket = socket;
	this.input = socket.getInputStream();
	this.output = socket.getOutputStream();

	this.isOpen = true;
    }

    public void run() {

	while (this.isOpen) {

	    try {

		ProtocolMessage request = this.receiveMessage();

		if (request.getStatus() == StatusType.PUT) {
		    try {
			StatusType response_status = this.kvServer.putKV(request.getKey(), request.getValue());
			this.sendMessage(response_status, null);
		    } catch (Exception e) {

			this.logger.error(e.toString());

			if (request.getValue() == null) {
			    this.sendMessage(StatusType.DELETE_ERROR, null);
			} else {
			    this.sendMessage(StatusType.PUT_ERROR, null);
			}

		    }
		} else {
		    
		    try {

			String value = this.kvServer.getKV(request.getKey());

			if (value == null) {
			    this.sendMessage(StatusType.GET_ERROR, null);
			} else {
			    this.sendMessage(StatusType.GET_SUCCESS, value);
			}
		    	
		    } catch (Exception e) {
			this.logger.error(e.toString());
			this.sendMessage(StatusType.GET_ERROR, null);
		    }

		}

	    } catch(Exception e) {
		this.logger.error(e.toString());
		return;
	    }

	}

    }

    private ProtocolMessage receiveMessage() throws ClassNotFoundException, IOException {
	ObjectInputStream ois = new ObjectInputStream(this.input);

	ProtocolMessage request = (ProtocolMessage) ois.readObject();
	ois.skipBytes(2);


	this.logger.info("Received protocol message: status = " + request.getStatus() + ", key = " + request.getKey() + ", value = " + request.getValue()); 

	return request;
    }

    private void sendMessage(StatusType status, String value) throws IOException {
	
	ProtocolMessage response = new ProtocolMessage(status, null, value);

	ObjectOutputStream oos = new ObjectOutputStream(this.output);

	oos.writeObject(response);
	oos.write('\r');
	oos.write('\n');
	oos.flush();

	this.logger.info("Sent protocol message: Put response with status = " + response.getStatus());
    }
}
