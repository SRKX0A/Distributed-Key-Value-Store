package app_kvServer;

import java.io.*;
import java.net.*;
import java.security.*;

import org.apache.log4j.Logger;

import client.ProtocolMessage;
import shared.KeyRange;
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

		if (this.kvServer.getServerState() != KVServer.ServerState.SERVER_AVAILABLE) {
		    this.sendMessage(StatusType.SERVER_STOPPED, null, null);
		    continue;
		}

		if (request.getStatus() == StatusType.PUT) {
		    this.handlePutRequest(request);
		} else if (request.getStatus() == StatusType.GET) {
		    this.handleGetRequest(request); 
		} else if (request.getStatus() == StatusType.KEYRANGE) {
		    this.handleKeyrangeRequest(request);
		} else {
		    this.handleInvalidMessageRequestType();
		}

	    } catch (IllegalArgumentException iae) {
		this.handleIllegalArgumentException(iae);
	    } catch (EOFException eofe) {
		this.handleEOFException(eofe);
		return;
	    } catch(Exception e) {
		this.handleGeneralException(e);
		return;
	    } 

	}

    }

    public void handlePutRequest(KVMessage request) throws Exception {
	try {

	    if (this.kvServer.getServerState() == KVServer.ServerState.SERVER_REBALANCING) {
		this.sendMessage(StatusType.SERVER_WRITE_LOCK, null, null);
		return;
	    }

	    KeyRange serverKeyRange = this.kvServer.getMetadata().get(this.hashIP(this.kvServer.getHostname(), this.kvServer.getPort()));

	    if (!serverKeyRange.withinKeyRange(this.hashKey(request.getKey()))) {
		this.sendMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
		return;
	    }

	    StatusType response_status = this.kvServer.putKV(request.getKey(), request.getValue());
	    this.sendMessage(response_status, request.getKey(), request.getValue());
	} catch (Exception e) {
	    logger.error("Failure in handling PUT request: " + e.toString());
	    this.sendMessage(StatusType.PUT_ERROR, request.getKey(), request.getValue());
	}
    }

    public void handleGetRequest(KVMessage request) throws Exception {
	try {

	    KeyRange serverKeyRange = this.kvServer.getMetadata().get(this.hashIP(this.kvServer.getHostname(), this.kvServer.getPort()));

	    if (!serverKeyRange.withinKeyRange(this.hashKey(request.getKey()))) {
		this.sendMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
		return;
	    }

	    String value = this.kvServer.getKV(request.getKey());

	    if (value.equals("null")) {
		this.sendMessage(StatusType.GET_ERROR, request.getKey(), null);
	    } else {
		this.sendMessage(StatusType.GET_SUCCESS, request.getKey(), value);
	    }

	} catch (Exception e) {
	    logger.error("Failure to handle GET request: " + e.toString());
	    this.sendMessage(StatusType.GET_ERROR, request.getKey(), null);
	}
    }

    public void handleKeyrangeRequest(KVMessage request) throws Exception {
	try {
	    this.sendMessage(StatusType.KEYRANGE_SUCCESS, this.kvServer.getKeyRangeSuccessString(), null);
	} catch (Exception e) {
	    logger.error("Failure to handle KEYRANGE request: " + e.toString());
	    this.sendMessage(StatusType.SERVER_STOPPED, null, null);
	}
    }

    public void handleInvalidMessageRequestType() throws Exception {
	logger.error("Client message format failure: Invalid request status.");
	this.sendMessage(StatusType.FAILED, "Error: Message request must be either PUT or GET.", "Error: Message request must be either PUT or GET.");
    }

    public void handleIllegalArgumentException(IllegalArgumentException iae) {
	logger.error("Client message format failure: " + iae.toString());

	try {
	    this.sendMessage(StatusType.FAILED, iae.toString(), iae.toString());
	} catch (Exception e) {
	    logger.error("Failed to send failure message: " + e.toString()); 
	}
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

    private ProtocolMessage receiveMessage() throws IllegalArgumentException, IOException, Exception {

	int byteCount = 0;
	int index = 0;
	byte[] msgBuf = new byte[BUFFER_SIZE];

	byte prev_value = 0;
	byte cur_value = 0;

	while ((cur_value = (byte) this.input.read()) != -1) {

	    msgBuf[byteCount++] = cur_value;
	    index++;

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

	if (cur_value == -1 && byteCount == 0) {
	    throw new EOFException("EOF reached");
	}

	byte[] tmpBuf = new byte[byteCount];
	System.arraycopy(msgBuf, 0, tmpBuf, 0, byteCount);
	msgBuf = tmpBuf;

	ProtocolMessage request = ProtocolMessage.fromBytesAtServer(msgBuf);

	logger.info(String.format("Received protocol message: status = %s, key = %s, value = %s", request.getStatus(), request.getKey(), request.getValue())); 

	return request;
    }

    private void sendMessage(StatusType status, String key, String value) throws Exception {
	
	ProtocolMessage response = new ProtocolMessage(status, key, value);
	this.output.write(response.getBytes());
	this.output.flush();

	logger.info(String.format("Sent protocol message: status = %s, key = %s, value = %s", response.getStatus(), response.getKey(), response.getValue()));
    }

    private byte[] hashIP(String address, int port) {
	try {
	    String valueToHash = address + ":" + Integer.toString(port);	
	    MessageDigest md = MessageDigest.getInstance("MD5");
	    md.update(valueToHash.getBytes());
	    return md.digest();
	} catch (Exception e) {
	    throw new RuntimeException("Error: Impossible NoSuchAlgorithmError!");
	}
    }

    private byte[] hashKey(String key) {
	try {;	
	    MessageDigest md = MessageDigest.getInstance("MD5");
	    md.update(key.getBytes());
	    return md.digest();
	} catch (Exception e) {
	    throw new RuntimeException("Error: Impossible NoSuchAlgorithmError!");
	}
    }

}
