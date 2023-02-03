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

    public Connection(Socket socket, KVServer kvServer) throws IOException {
	this.kvServer = kvServer;

	this.socket = socket;
	this.input = socket.getInputStream();
	this.output = socket.getOutputStream();

    }

    public void run() {

	while (true) {

	    try {

		ProtocolMessage request = this.receiveMessage();

		if (request.getStatus() == StatusType.PUT) {
		    try {
			StatusType response_status = this.kvServer.putKV(request.getKey(), request.getValue());
			this.sendMessage(response_status, request.getKey(), request.getValue());
		    } catch (Exception e) {
			this.logger.error("Failure in handling PUT request: " + e.toString());
			this.sendMessage(StatusType.PUT_ERROR, request.getKey(), request.getValue());
		    }
		} else if (request.getStatus() == StatusType.GET) {
		    
		    try {

			String value = this.kvServer.getKV(request.getKey());

			if (value.equals("null")) {
			    this.sendMessage(StatusType.GET_ERROR, request.getKey(), null);
			} else {
			    this.sendMessage(StatusType.GET_SUCCESS, request.getKey(), value);
			}
		    	
		    } catch (Exception e) {
			this.logger.error("Failure to handle GET request: " + e.toString());
			this.sendMessage(StatusType.GET_ERROR, request.getKey(), null);
		    }
		} else {
		    
		    this.logger.error("Client message format failure: Invalid request status.");
		    this.sendMessage(StatusType.FAILED, "Error: Message request must be either PUT or GET.", "Error: Message request must be either PUT or GET.");

		}

	    } catch (ClassNotFoundException cnfe) {
	    
		this.logger.error("Client message format failure: " + cnfe.toString());

		try {
		    this.sendMessage(StatusType.FAILED, "Error: Message format unknown.", "Error: Message format unknown.");
		} catch (Exception e) {
		    this.logger.error("Failed to send failure message: " + e.toString()); 
		    return;
		}


	    } catch(Exception e) {
		this.logger.error("Client connection failure: " + e.toString());

		try {
		    this.input.close();
		    this.output.close();
		    this.socket.close();
		} catch (IOException ioe) {
		    this.logger.error("Failed to gracefully close connection: " + ioe.toString()); 
		}

		return;

	    } 

	}

    }

    private ProtocolMessage receiveMessage() throws ClassNotFoundException, IOException {
	ObjectInputStream ois = new ObjectInputStream(this.input);

	ProtocolMessage request = (ProtocolMessage) ois.readObject();
	ois.skipBytes(2);


	this.logger.info(String.format("Received protocol message: status = %s, key = %s, value = %s", request.getStatus(), request.getKey(), request.getValue())); 

	return request;
    }

    private void sendMessage(StatusType status, String key, String value) throws IOException {
	
	ProtocolMessage response = new ProtocolMessage(status, key, value);

	ObjectOutputStream oos = new ObjectOutputStream(this.output);

	oos.writeObject(response);
	oos.write('\r');
	oos.write('\n');
	oos.flush();

	this.logger.info(String.format("Sent protocol message: PUT response with status = %s, key = %s, value = %s", response.getStatus(), response.getKey(), response.getValue()));
    }
}
