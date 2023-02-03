package app_kvServer;

import java.io.*;
import java.net.*;

import org.apache.log4j.Logger;

import client.ProtocolMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class Connection extends Thread {
    
    private static Logger logger = Logger.getRootLogger();

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    
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

	    } catch (IllegalArgumentException iae) {
		this.logger.error("Client message format failure: " + iae.toString());

		try {
		    this.sendMessage(StatusType.FAILED, iae.toString(), iae.toString());
		} catch (Exception e) {
		    this.logger.error("Failed to send failure message: " + e.toString()); 
		}

	    } catch(Exception e) {
		this.logger.error("Client connection failure: " + e.toString());

		try {
		    this.output.close();
		    this.input.close();
		    this.socket.close();
		} catch (IOException ioe) {
		    this.logger.error("Failed to gracefully close connection: " + ioe.toString()); 
		}

		return;

	    } 

	}

    }

    private ProtocolMessage receiveMessage() throws IllegalArgumentException, IOException, Exception {

	int byteCount = 0;
	int index = 0;
	byte[] msgBuf = new byte[BUFFER_SIZE];

	byte prev_value = 0;
	byte cur_value = 0;

	while ((cur_value = (byte) this.input.read()) != -1) {

	    msgBuf[index++] = cur_value;
	    byteCount++;

	    if (byteCount > DROP_SIZE) {
		break;
	    }

	    if (prev_value == 13 && cur_value == 10) {
		break;
	    }

	    if (index == BUFFER_SIZE) {
		byte[] tmpBuf = new byte[BUFFER_SIZE + byteCount];
		System.arraycopy(msgBuf, 0, tmpBuf, 0, byteCount);
		msgBuf = tmpBuf;
		index = 0;
	    }

	    prev_value = cur_value;

	}

	byte[] tmpBuf = new byte[byteCount];
	System.arraycopy(msgBuf, 0, tmpBuf, 0, byteCount);
	msgBuf = tmpBuf;

	ProtocolMessage request = new ProtocolMessage(msgBuf);

	this.logger.info(String.format("Received protocol message: status = %s, key = %s, value = %s", request.getStatus(), request.getKey(), request.getValue())); 

	return request;
    }

    private void sendMessage(StatusType status, String key, String value) throws Exception {
	
	ProtocolMessage response = new ProtocolMessage(status, key, value);
	this.output.write(response.getBytes());
	this.output.flush();

	this.logger.info(String.format("Sent protocol message: PUT response with status = %s, key = %s, value = %s", response.getStatus(), response.getKey(), response.getValue()));
    }
}
