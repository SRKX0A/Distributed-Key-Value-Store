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

		ProtocolMessage request = receiveMessage(this.input);

		if (this.kvServer.getServerState() == KVServer.ServerState.SERVER_UNAVAILABLE) {
		    sendMessage(this.output, StatusType.SERVER_STOPPED, null, null);
		    continue;
		}

		if (this.kvServer.getServerState() == KVServer.ServerState.SERVER_INITIALIZING &&
		    request.getStatus() != StatusType.SERVER_INIT &&
		    request.getStatus() != StatusType.REPLICATE_KV_HANDSHAKE) {
		    sendMessage(this.output, StatusType.SERVER_STOPPED, null, null);
		    continue;
		}

		if (request.getStatus() == StatusType.PUT) {
		    this.handlePutRequest(request);
		} else if (request.getStatus() == StatusType.GET) {
		    this.handleGetRequest(request); 
		} else if (request.getStatus() == StatusType.KEYRANGE) {
		    this.handleKeyrangeRequest(request);
		} else if (request.getStatus() == StatusType.KEYRANGE_READ) {
		    this.handleKeyrangereadRequest(request);
		} else if (request.getStatus() == StatusType.SUBSCRIBE) {
		    this.handleSubscribeMessage(request);
		} else if (request.getStatus() == StatusType.UNSUBSCRIBE) {
		    this.handleUnsubscribeMessage(request);
		} else if (request.getStatus() == StatusType.SERVER_INIT) {
		    this.handleServerInitMessage();
		    return;
		} else if (request.getStatus() == StatusType.REPLICATE_KV_HANDSHAKE) {
		    this.handleReplicateKVHandshakeMessage(request);
		    return;
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
		sendMessage(this.output, StatusType.SERVER_WRITE_LOCK, null, null);
		return;
	    }

	    KeyRange serverKeyRange = this.kvServer.getMetadata().get(this.hashIP(this.kvServer.getHostname(), this.kvServer.getPort()));

	    if (!serverKeyRange.withinKeyRange(this.hashKey(request.getKey()))) {
		sendMessage(this.output, StatusType.SERVER_NOT_RESPONSIBLE, null, null);
		return;
	    }

	    StatusType response_status = this.kvServer.putKV(request.getKey(), request.getValue());
	    sendMessage(this.output, response_status, request.getKey(), request.getValue());
	} catch (Exception e) {
	    logger.error("Failure in handling PUT request: " + e.toString());
	    sendMessage(this.output, StatusType.PUT_ERROR, request.getKey(), request.getValue());
	}
    }

    public void handleGetRequest(KVMessage request) throws Exception {
	try {

	    if (!this.isServerResponsibleForGetKey(request.getKey())) {
		sendMessage(this.output, StatusType.SERVER_NOT_RESPONSIBLE, null, null);
		return;
	    }

	    String value = this.kvServer.getKV(request.getKey());

	    if (value.equals("null")) {
		sendMessage(this.output, StatusType.GET_ERROR, request.getKey(), null);
	    } else {
		sendMessage(this.output, StatusType.GET_SUCCESS, request.getKey(), value);
	    }

	} catch (Exception e) {
	    logger.error("Failure to handle GET request: " + e.toString());
	    sendMessage(this.output, StatusType.GET_ERROR, request.getKey(), null);
	}
    }

    public void handleKeyrangeRequest(KVMessage request) throws Exception {
	try {
	    sendMessage(this.output, StatusType.KEYRANGE_SUCCESS, this.kvServer.getKeyRangeSuccessString(), null);
	} catch (Exception e) {
	    logger.error("Failure to handle KEYRANGE request: " + e.toString());
	    sendMessage(this.output, StatusType.SERVER_STOPPED, null, null);
	}
    }

    public void handleKeyrangereadRequest(KVMessage request) throws Exception {
	try {
	    sendMessage(this.output, StatusType.KEYRANGE_READ_SUCCESS, this.kvServer.getKeyRangeReadSuccessString(), null);
	} catch (Exception e) {
	    logger.error("Failure to handle KEYRANGE_READ request: " + e.toString());
	    sendMessage(this.output, StatusType.SERVER_STOPPED, null, null);
	}
    }

    public void handleSubscribeMessage(KVMessage request) throws Exception {

	if (!this.isServerResponsibleForGetKey(request.getKey())) {
	    sendMessage(this.output, StatusType.SERVER_NOT_RESPONSIBLE, null, null);
	    return;
	}

	String[] addressAndPort = request.getValue().split(":");
	String address = addressAndPort[0];
	int port = Integer.parseInt(addressAndPort[1]);
    
	if (this.kvServer.subscribeClient(address, port, request.getKey())) {
	    sendMessage(this.output, StatusType.SUBSCRIBE_SUCCESS, null, null);
	} else {
	    sendMessage(this.output, StatusType.SUBSCRIBE_ERROR, null, null);	
	}
	    
    }
	
    public void handleUnsubscribeMessage(KVMessage request) throws Exception {

	if (!this.isServerResponsibleForGetKey(request.getKey())) {
	    sendMessage(this.output, StatusType.SERVER_NOT_RESPONSIBLE, null, null);
	    return;
	}

	String[] addressAndPort = request.getValue().split(":");
	String address = addressAndPort[0];
	int port = Integer.parseInt(addressAndPort[1]);

    	if (this.kvServer.unsubscribeClient(address, port, request.getKey())) {
	    sendMessage(this.output, StatusType.UNSUBSCRIBE_SUCCESS, null, null);
	} else {
	    sendMessage(this.output, StatusType.UNSUBSCRIBE_ERROR, null, null);	
	}
    
    }


    public void handleServerInitMessage() throws Exception {
	new ServerConnection(this.socket, this.kvServer).start();
    }

    public void handleReplicateKVHandshakeMessage(KVMessage request) throws Exception {
	
	String currentTopology = this.kvServer.getKeyRangeSuccessString();
	String senderTopology = request.getKey();

	if (this.kvServer.getServerState() != KVServer.ServerState.SERVER_INITIALIZING && !currentTopology.equals(senderTopology)) {
	    logger.warn("Replication request denied due to differing topology");
	    sendMessage(this.output, StatusType.REPLICATE_KV_HANDSHAKE_NACK, null, null);
	} else {
	    sendMessage(this.output, StatusType.REPLICATE_KV_HANDSHAKE_ACK, null, null);
	    new ServerConnection(this.socket, this.kvServer).start();
	}

	return;

    }
	
    public void handleInvalidMessageRequestType() throws Exception {
	logger.error("Client message format failure: Invalid request status.");
	sendMessage(this.output, StatusType.FAILED, "Error: Message request must be either PUT or GET.", "Error: Message request must be either PUT or GET.");
    }

    public void handleIllegalArgumentException(IllegalArgumentException iae) {
	logger.error("Client message format failure: " + iae.toString());

	try {
	    sendMessage(this.output, StatusType.FAILED, iae.toString(), iae.toString());
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

    public static ProtocolMessage receiveMessage(InputStream input) throws IllegalArgumentException, IOException, Exception {

	int byteCount = 0;
	int index = 0;
	byte[] msgBuf = new byte[BUFFER_SIZE];

	byte prev_value = 0;
	byte cur_value = 0;

	while ((cur_value = (byte) input.read()) != -1) {

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

    public static void sendMessage(OutputStream output, StatusType status, String key, String value) throws Exception {
	
	ProtocolMessage response = new ProtocolMessage(status, key, value);
	output.write(response.getBytes());
	output.flush();

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

    private boolean isServerResponsibleForGetKey(String key) {

	var metadata = this.kvServer.getMetadata();

	KeyRange serverKeyRange = metadata.get(this.hashIP(this.kvServer.getHostname(), this.kvServer.getPort()));

	var firstPredecessorEntry = metadata.lowerEntry(serverKeyRange.getRangeFrom());

	if (firstPredecessorEntry == null) {
	    firstPredecessorEntry = metadata.lastEntry();
	}

	var secondPredecessorEntry = metadata.lowerEntry(firstPredecessorEntry.getKey());

	if (secondPredecessorEntry == null) {
	    secondPredecessorEntry = metadata.lastEntry();
	}

	byte[] requestKeyHash = this.hashKey(key);

	return serverKeyRange.withinKeyRange(requestKeyHash) ||
		firstPredecessorEntry.getValue().withinKeyRange(requestKeyHash) ||
		secondPredecessorEntry.getValue().withinKeyRange(requestKeyHash);

    }

}
