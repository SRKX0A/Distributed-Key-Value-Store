package app_kvServer;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;

import org.apache.log4j.Logger;

import shared.KeyRange;
import shared.messages.ECSMessage;
import shared.messages.KVMessage;

public class ECSConnection extends Thread {

    private static Logger logger = Logger.getRootLogger();

    private KVServer kvServer;
    
    private Socket socket;
    private ObjectInputStream input;
    private ObjectOutputStream output;

    private volatile boolean finishedShutdown;

    public ECSConnection(KVServer kvServer) throws Exception {
	this.kvServer = kvServer;
	this.socket = new Socket(this.kvServer.getECSAddress(), this.kvServer.getECSPort());
	this.output = new ObjectOutputStream(this.socket.getOutputStream());
	this.input = new ObjectInputStream(this.socket.getInputStream());
    }

    @Override
    public void run() {

	try {
	    this.sendMessage(ECSMessage.StatusType.INIT_REQ, this.kvServer.getHostname(), this.kvServer.getPort(), null, null);
	} catch (Exception e) {
	    logger.error("Failed to send startup message to ECS: " + e.getMessage());
	    this.kvServer.close();
	    return;
	}

	while (true) {

	    try {

		ECSMessage msg = this.receiveMessage();

		if (msg.getStatus() == ECSMessage.StatusType.METADATA_UPDATE) {
		    this.handleMetadataUpdate(msg);
		} else if (msg.getStatus() == ECSMessage.StatusType.METADATA_LOCK) {
		    this.handleMetadataLock(msg);
		} else if (msg.getStatus() == ECSMessage.StatusType.SHUTDOWN) {
		    this.handleServerShutdown(msg);
		    this.finishedShutdown = true;
		    return;
		}

	    } catch (Exception e) {
		logger.error("ECS connection failure: " + e.getMessage());
		this.kvServer.close();
		return;
	    }

	}

    }

    public void handleMetadataUpdate(ECSMessage message) throws Exception {

	if (this.kvServer.getServerState() == KVServer.ServerState.SERVER_INITIALIZING) {
	    logger.debug("Got initial metadata update from ECS, turning state to available");
	    this.kvServer.recoverIfNecessary(message);
	    this.kvServer.setMetadata(message.getMetadata());
	    this.kvServer.setServerState(KVServer.ServerState.SERVER_AVAILABLE);
	    this.kvServer.startReplicationTimer();
	} else {
	    logger.debug("Got metadata update from ECS");
	    this.kvServer.recoverIfNecessary(message);
	    this.kvServer.setMetadata(message.getMetadata());
	    this.kvServer.setServerState(KVServer.ServerState.SERVER_AVAILABLE);
	}

    }

    public void handleMetadataLock(ECSMessage message) throws Exception {

	String serverAddress = message.getAddress();
	int serverPort = message.getPort();

	if (serverAddress == null && serverPort == 0) {
	    logger.debug("Got single termination message from ECS");
	    return;
	}

	//this.kvServer.setMetadata(message.getMetadata());
	this.kvServer.setServerState(KVServer.ServerState.SERVER_REBALANCING);

	logger.debug("Got write lock message from ECS");

	this.kvServer.dumpCacheToDisk();
	this.kvServer.compactLogs();
	this.kvServer.clearOldLogs();
	this.kvServer.partitionLogsByKeyRange();
	this.kvServer.sendFilteredLogsToServer(serverAddress, serverPort);
	this.kvServer.clearFilteredLogs();
	
	try {
	    this.sendMessage(ECSMessage.StatusType.REQ_FIN, this.kvServer.getHostname(), this.kvServer.getPort(), null, null);
	} catch (Exception e) {
	    logger.error("Failed to send REQ_FIN messsage to ECS: " + e.getMessage());
	}

	Thread.sleep(100);

	return;

    }

    public void handleServerShutdown(ECSMessage message) throws Exception {

	String serverAddress = message.getAddress();
	int serverPort = message.getPort();

	if (serverAddress == null && serverPort == 0) {
	    logger.debug("Got single termination message from ECS");
	    return;
	}

	logger.debug("Got shutdown message from ECS");

	this.kvServer.setServerState(KVServer.ServerState.SERVER_UNAVAILABLE);
	
	try {
	    this.sendMessage(ECSMessage.StatusType.REQ_FIN, this.kvServer.getHostname(), this.kvServer.getPort(), null, null);
	} catch (Exception e) {
	    logger.error("Failed to send REQ_FIN messsage to ECS: " + e.getMessage());
	}

	Thread.sleep(100);

	return;

    }

    public void shutdown() {

	try {
	    this.kvServer.setServerState(KVServer.ServerState.SERVER_REBALANCING);
	    this.kvServer.stopReplicationTimer();
	    this.kvServer.replicate();
	    this.sendMessage(ECSMessage.StatusType.TERM_REQ, this.kvServer.getHostname(), this.kvServer.getPort(), null, null);
	} catch (Exception e) {
	    logger.error("Failed to send TERM_REQ message to ECS: " + e.getMessage());
	    return;
	}

	while (!this.finishedShutdown);

	this.kvServer.clearOldReplicatedLogs(KVMessage.StatusType.REPLICATE_KV_1);
	this.kvServer.clearOldReplicatedLogs(KVMessage.StatusType.REPLICATE_KV_2);

    }

    public Socket getSocket() {
	return this.socket;
    }

    public void sendMessage(ECSMessage.StatusType status, String address, int port, TreeMap<byte[], KeyRange> metadata, byte[] ringPosition) throws Exception {

	ECSMessage response = new ECSMessage(status, address, port, metadata, ringPosition);

	this.output.writeObject(response);
	this.output.flush();

	logger.debug(String.format("Server sent request to ECS with status = %s\n", response.getStatus()));

	return;

    }

    public ECSMessage receiveMessage() throws Exception {
	
	ECSMessage request = (ECSMessage) this.input.readObject();

	logger.debug(String.format("Server <%s:%d> received response from ECS with status = %s\n", request.getAddress(), request.getPort(), request.getStatus()));

	return request;

    }
}
