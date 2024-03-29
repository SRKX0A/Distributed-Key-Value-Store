package app_kvECS;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;

import org.apache.log4j.Logger;

import shared.KeyRange;
import shared.messages.ECSMessage;
import shared.messages.ECSMessage.StatusType;

public class ECSClientConnection extends Thread {

    private static Logger logger = Logger.getRootLogger();

    private ECS ecs;

    private Socket socket;
    private ObjectInputStream input;
    private ObjectOutputStream output;

    private String serverAddress;
    private int serverPort;

    public ECSClientConnection(Socket socket, ECS ecs) throws IOException {
	this.ecs = ecs;

	this.socket = socket;
	this.output = new ObjectOutputStream(this.socket.getOutputStream());
	this.input = new ObjectInputStream(this.socket.getInputStream());

    }

    @Override 
    public void run() {

	while (true) {

	    try {

		ECSMessage request = this.receiveMessage();

		if (request.getStatus() == StatusType.INIT_REQ) {
		    this.handleInitialization(request);
		} else if (request.getStatus() == StatusType.TERM_REQ) {
		    this.handleTermination(request);
		    return;
		} else {
		    throw new IllegalArgumentException("Error: Server messages must be one of INIT_REQ, TERM_REQ, or REQ_FIN");
		}

	    } catch(IllegalArgumentException iae) {
		this.handleIllegalArgumentException(iae);
	    } catch (ClassNotFoundException cnfe) {
		this.handleClassNotFoundException(cnfe);
	    } catch (EOFException eofe) {
		this.handleEOFException(eofe);
		return;
	    } catch (Exception e) {
		this.handleGeneralException(e);	
		return;
	    }


	}

    }

    /**
     * Handle initialization request from server. Adds server to ECS and updates all nodes
     * with the new metadata, after imposing a write-lock on another server for KV transferring. 
     * If a failure occurs, the server which fails is removed from both the metadata and the connection list.
     * @param request The ECSMessage request the ECS receives from the server
     */
    public void handleInitialization(ECSMessage request) {

	synchronized(ECS.class) {

	    this.serverAddress = request.getAddress();
	    this.serverPort = request.getPort();

	    if (this.ecs.getNumNodes() == 0) {
		KeyRange serverKeyRange = this.ecs.addNode(this.serverAddress, this.serverPort);
		this.ecs.getConnections().put(serverKeyRange.getRangeFrom(), this);
		this.sendUpdateMessageToAllNodes(this.serverAddress, this.serverPort);
		return;
	    }

	    byte[] ringPosition = this.hashIP(this.serverAddress, this.serverPort);
	    var successorEntry = this.ecs.getMetadata().higherEntry(ringPosition);

	    if (successorEntry == null) {
		successorEntry = this.ecs.getMetadata().firstEntry();
	    }

	    byte[] successorRingPosition = successorEntry.getKey();
	    KeyRange successorKeyRange = successorEntry.getValue();

	    ECSClientConnection connection = this.ecs.getConnections().get(successorRingPosition);

	    try {
		connection.sendMessage(StatusType.METADATA_LOCK, this.serverAddress, this.serverPort, this.ecs.getMetadata(), successorRingPosition);
	    } catch (Exception e) {
		logger.error("Failed to send write lock message to successor: " + e.getMessage());
		this.removeNodeFromECSAndConnectionList(successorKeyRange);
		this.sendUpdateMessageToAllNodes(this.serverAddress, this.serverPort);
		this.handleInitialization(request);
		return;
	    }

	    try {
		ECSMessage response = this.receiveMessage();
		if (response.getStatus() != StatusType.REQ_FIN) {
		    throw new IllegalArgumentException("Invalid response - expecting REQ_FIN, got " + response.getStatus().toString());
		}
	    } catch (Exception e) {
		logger.error("Failed to receive REQ_FIN message from initializing server: " + e.getMessage());
		return;
	    }

	    KeyRange serverKeyRange = this.ecs.addNode(this.serverAddress, this.serverPort);
	    this.ecs.getConnections().put(serverKeyRange.getRangeFrom(), this);
	    this.sendUpdateMessageToAllNodes(this.serverAddress, this.serverPort);
	    
	    return;

	}

    }

    public void handleTermination(ECSMessage request) {

	synchronized (ECS.class) {

	    String address = request.getAddress();
	    int port = request.getPort();

	    KeyRange serverKeyRange = this.ecs.removeNode(address, port);
	    byte[] serverRingPosition = serverKeyRange.getRangeFrom();
	    TreeMap<byte[], KeyRange> updatedMetadata = this.ecs.getMetadata();

	    if (this.ecs.getNumNodes() == 0) {
		try {
		    this.sendMessage(StatusType.SHUTDOWN, null, 0, null, null);
		} catch (Exception e) {
		    logger.error("Failed to send write lock message to single terminating node: " + e.getMessage());
		}
		this.ecs.getConnections().remove(serverRingPosition);
		return;
	    }

	    byte[] successorRingPosition = updatedMetadata.higherKey(serverKeyRange.getRangeFrom());

	    if (successorRingPosition == null) {
		successorRingPosition = updatedMetadata.firstKey();
	    }

	    KeyRange successorNodeRange = updatedMetadata.get(successorRingPosition);

	    String successorAddress = successorNodeRange.getAddress();
	    int successorPort = successorNodeRange.getPort();

	    try {
		this.sendMessage(StatusType.SHUTDOWN, successorAddress, successorPort, updatedMetadata, serverRingPosition);
	    } catch (Exception e) {
		logger.error("Failed to send write lock message to terminating node: " + e.getMessage());
		this.ecs.getConnections().remove(serverRingPosition);
		this.sendUpdateMessageToAllNodes(address, port);
		return;
	    }

	    ECSMessage writeLockResponse = null;

	    try {
		writeLockResponse = this.receiveMessage(); 
		if (writeLockResponse.getStatus() != StatusType.REQ_FIN) {
		    throw new IllegalArgumentException("Expecting REQ_FIN message");	
		}
	    } catch (Exception e) {
		logger.error("Failed to receive write lock response from terminating node: " + e.getMessage());
	    } finally {
		this.ecs.getConnections().remove(serverRingPosition);
		this.sendUpdateMessageToAllNodes(address, port);
	    }
	}

    }

    public void handleIllegalArgumentException(IllegalArgumentException iae) {
	
	synchronized (ECS.class) {
	    logger.error("Client message format failure: " + iae.toString());

	    try {
		this.sendMessage(StatusType.INVALID_REQUEST_TYPE, null, 0, null, null);
	    } catch (Exception e) {
		logger.error("Failed to send failure message: " + e.toString()); 
	    }
	}

    }

    public void handleClassNotFoundException(ClassNotFoundException cnfe) {

	synchronized (ECS.class) {
	    logger.error("Client message format failure: " + cnfe.toString());

	    try {
		this.sendMessage(StatusType.INVALID_MESSAGE_FORMAT, null, 0, null, null);
	    } catch (Exception e) {
		logger.error("Failed to send failure message: " + e.toString()); 
	    }
	}

    }

    public void handleEOFException(EOFException eofe) {

	synchronized (ECS.class) {
	    logger.debug("Client connection closed: " + eofe.getMessage());

	    try {
		this.input.close();
		this.output.close();	
		this.socket.close();
	    } catch (Exception e) {
		logger.error("Error: Failed to gracefully close connection: " + e.getMessage());
	    } finally {

		if (this.serverAddress == null || this.serverPort == 0) {
		    return;
		}

		byte[] hashedValue = this.hashIP(this.serverAddress, this.serverPort);
		this.ecs.getConnections().remove(hashedValue);
		this.ecs.removeNode(this.serverAddress, this.serverPort);

		String sentAddress = this.serverAddress;
		int sentPort = this.serverPort;

		this.sendUpdateMessageToAllNodes(sentAddress, sentPort);

	    }
	}

    }

    public void handleGeneralException(Exception e) {
	
	synchronized (ECS.class) {
	    logger.debug("Client connection failure: " + e.getMessage());

	    try {
		this.input.close();
		this.output.close();	
		this.socket.close();
	    } catch (Exception ex) {
		logger.error("Error: Failed to gracefully close connection: " + ex.getMessage());
	    } finally {

		if (this.serverAddress == null || this.serverPort == 0) {
		    return;
		}

		byte[] hashedValue = this.hashIP(this.serverAddress, this.serverPort);
		this.ecs.getConnections().remove(hashedValue);
		this.ecs.removeNode(this.serverAddress, this.serverPort);

		String sentAddress = this.serverAddress;
		int sentPort = this.serverPort;

		this.sendUpdateMessageToAllNodes(sentAddress, sentPort);
	    }
	}

    }

    public void sendMessage(StatusType status, String address, int port, TreeMap<byte[], KeyRange> metadata, byte[] ringPosition) throws Exception {

	ECSMessage response = new ECSMessage(status, address, port, metadata, ringPosition);

	this.output.writeObject(response);
	this.output.flush();
	this.output.reset();

	logger.debug(String.format("ECS sent response with status = %s\n", response.getStatus()));

	return;

    }

    public ECSMessage receiveMessage() throws Exception {
	
	ECSMessage request = (ECSMessage) this.input.readObject();

	logger.debug(String.format("ECS received request from node <%s:%d> with status = %s\n", request.getAddress(), request.getPort(), request.getStatus()));

	return request;

    }

    public void sendUpdateMessageToAllNodes(String address, int port) {

	var entryIterator = this.ecs.getConnections().entrySet().iterator();

	while (entryIterator.hasNext()) {

	    var entry = entryIterator.next();

	    byte[] ringPosition = entry.getKey(); 
	    ECSClientConnection connection = entry.getValue();

	    KeyRange nodeRange = this.ecs.getMetadata().get(ringPosition);

	    try {
		connection.sendMessage(StatusType.METADATA_UPDATE, address, port, this.ecs.getMetadata(), ringPosition);
	    } catch (Exception e) {
		logger.error(String.format("Failed to update node <%s:%d>: %s\n", nodeRange.getAddress(), nodeRange.getPort(), e.getMessage()));
		this.removeNodeFromECSAndConnectionList(nodeRange);
		this.sendUpdateMessageToAllNodes(address, port);
		return;
	    }

	}

    }

    private void removeNodeFromECSAndConnectionList(KeyRange nodeRange) {
	this.ecs.removeNode(nodeRange.getAddress(), nodeRange.getPort());
	this.ecs.getConnections().remove(nodeRange.getRangeFrom());
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

}
