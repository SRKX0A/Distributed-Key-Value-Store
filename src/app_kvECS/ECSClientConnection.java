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

    public class RequestPendingInfo {
	private String pendingAddress;
	private int pendingPort;

	public RequestPendingInfo(String pendingAddress, int pendingPort) {
	    this.pendingAddress = pendingAddress;
	    this.pendingPort = pendingPort;
	}

    }

    private static Logger logger = Logger.getRootLogger();

    private ECS ecs;

    private Socket socket;
    private ObjectInputStream input;
    private ObjectOutputStream output;

    private RequestPendingInfo info;

    private String serverAddress;
    private int serverPort;

    public ECSClientConnection(Socket socket, ECS ecs) throws IOException {
	this.ecs = ecs;

	this.socket = socket;
	this.output = new ObjectOutputStream(this.socket.getOutputStream());
	this.input = new ObjectInputStream(this.socket.getInputStream());

	this.info = null;
    }

    @Override 
    public void run() {

	while (true) {

	    try {

		ECSMessage request = this.receiveMessage();

		if (request.getStatus() == StatusType.REQ_FIN) {
		    this.handleRebalance(request);
		    continue;
		}

		this.serverAddress = request.getAddress();
		this.serverPort = request.getPort();

		synchronized (ECSClientConnection.class) {

		    if (request.getStatus() == StatusType.INIT_REQ) {
			this.handleInitialization(request);
		    } else if (request.getStatus() == StatusType.TERM_REQ) {
			this.handleTermination(request);
			return;
		    } else {
			throw new IllegalArgumentException("Error: Server messages must be one of INIT_REQ, TERM_REQ, or REQ_FIN");
		    }

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

	    String address = request.getAddress();
	    int port = request.getPort();

	    KeyRange serverKeyRange = this.ecs.addNode(address, port);
	    this.ecs.getConnections().put(serverKeyRange.getRangeFrom(), this);
	    TreeMap<byte[], KeyRange> updatedMetadata = this.ecs.getMetadata();

	    if (this.ecs.getNumNodes() == 1) {
		this.sendUpdateMessageToAllNodes(address, port);
		return;
	    }

	    byte[] successorRingPosition = updatedMetadata.higherKey(serverKeyRange.getRangeFrom());

	    if (successorRingPosition == null) {
		successorRingPosition = updatedMetadata.firstKey();
	    }

	    KeyRange successorNodeRange = updatedMetadata.get(successorRingPosition);

	    TreeMap<byte[], ECSClientConnection> connections = this.ecs.getConnections();

	    try {
		connections.get(successorRingPosition).info = new RequestPendingInfo(address, port);
		connections.get(successorRingPosition).sendMessage(StatusType.METADATA_LOCK, address, port, updatedMetadata, successorRingPosition);
	    } catch (Exception e) {
		logger.error("Failed to send write lock message to successor: " + e.getMessage());
		this.removeNodeFromECSAndConnectionList(successorNodeRange);
		this.sendUpdateMessageToAllNodes(address, port);
		return;
	    }
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
		    this.sendMessage(StatusType.METADATA_LOCK, null, 0, null, null);
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
		this.sendMessage(StatusType.METADATA_LOCK, successorAddress, successorPort, updatedMetadata, serverRingPosition);
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

    public void handleRebalance(ECSMessage message) throws Exception {

	synchronized (ECS.class) {
	    if (this.info == null) {
		throw new IllegalStateException("REQ_FIN request for connection that has not requested a rebalance!");
	    }

	    this.sendUpdateMessageToAllNodes(this.info.pendingAddress, this.info.pendingPort);
	    this.info = null;	
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
		byte[] hashedValue = this.hashIP(this.serverAddress, this.serverPort);
		this.ecs.getConnections().remove(hashedValue);
		this.ecs.removeNode(this.serverAddress, this.serverPort);

		String sentAddress = this.serverAddress;
		int sentPort = this.serverPort;

		if (this.info != null) {
		    sentAddress = this.info.pendingAddress;
		    sentPort = this.info.pendingPort;
		}

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
		byte[] hashedValue = this.hashIP(this.serverAddress, this.serverPort);
		this.ecs.getConnections().remove(hashedValue);
		this.ecs.removeNode(this.serverAddress, this.serverPort);

		String sentAddress = this.serverAddress;
		int sentPort = this.serverPort;

		if (this.info != null) {
		    sentAddress = this.info.pendingAddress;
		    sentPort = this.info.pendingPort;
		}

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
