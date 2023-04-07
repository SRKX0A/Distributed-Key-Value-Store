package app_kvServer;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.log4j.Logger;

import shared.messages.ServerMessage;
import shared.messages.ServerMessage.StatusType;
import client.ClientSubscriptionInfo;

public class ServerConnection extends Thread {

    private static Logger logger = Logger.getRootLogger();

    private KVServer kvServer;

    private Socket socket;

    private ObjectInputStream input;
    private ObjectOutputStream output;

    public ServerConnection(Socket socket, KVServer kvServer) throws Exception {
	this.kvServer = kvServer;
	this.socket = socket;
	this.output = new ObjectOutputStream(this.socket.getOutputStream());
	this.input = new ObjectInputStream(this.socket.getInputStream());
    }

    public void run() {

	try {

	    while (true) {
		
		ServerMessage request = receiveMessage(this.input);

		if (request.getStatus() == StatusType.SEND_KV) {
		    this.handleSendKVMessage(request);
		} else if (request.getStatus() == StatusType.SEND_REPLICA_KV_1 || request.getStatus() == StatusType.SEND_REPLICA_KV_2) {
		    this.handleReplicaKVMessage(request);
		} else if (request.getStatus() == StatusType.REPLICATE_KV_1 || request.getStatus() == StatusType.REPLICATE_KV_2) {
		    this.handleReplicateRequestMessage(request);
		} else if (request.getStatus() == StatusType.SEND_SUBSCRIPTIONS) {
		    this.handleSendSubscriptionsMessage(request);
		} else if (request.getStatus() == StatusType.REPLICATE_KV_1_FIN || request.getStatus() == StatusType.REPLICATE_KV_2_FIN) {
		    this.handleReplicateKVFinMessage(request);
		    return;
		} else if (request.getStatus() == StatusType.SERVER_INIT_FIN) {
		    this.handleServerInitFinMessage();
		    return;
		}

	    }

	} catch (EOFException eofe) {
	    this.handleEOFException(eofe);
	    return;
	} catch(Exception e) {
	    this.handleGeneralException(e);
	    return;
	}

    }

    public void handleSendKVMessage(ServerMessage request) {
	this.kvServer.getServerFileManager().writeFileFromFileContents("KVServerStoreFile_", request.getFileContents());	
    }

    public void handleReplicaKVMessage(ServerMessage request) {

	String prefix = null;
	if (request.getStatus() == StatusType.SEND_REPLICA_KV_1) {
	    prefix = "Replica1KVServerStoreFile_";
	} else {
	    prefix = "Replica2KVServerStoreFile_";
	}

	this.kvServer.getServerFileManager().writeFileFromFileContents(prefix, request.getFileContents());	

    }

    public void handleReplicateRequestMessage(ServerMessage request) {

	String prefix = null;
	if (request.getStatus() == StatusType.REPLICATE_KV_1) {
	    prefix = "NewReplica1KVServerStoreFile_";
	} else {
	    prefix = "NewReplica2KVServerStoreFile_";
	}

	this.kvServer.getServerFileManager().writeFileFromFileContents(prefix, request.getFileContents());	

    }

    public void handleReplicateKVFinMessage(ServerMessage request) {
	this.kvServer.getServerFileManager().clearOldReplicatedStoreFiles(request.getStatus());
    }

    public void handleSendSubscriptionsMessage(ServerMessage request) {
	this.kvServer.setSubscriptions(request.getSubscriptions());
    }

    public void handleServerInitFinMessage() {
	this.kvServer.getECSConnection().initializationFinished();
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

    public static void sendMessage(ObjectOutputStream output, StatusType status, byte[][] fileContents, TreeMap<String, List<ClientSubscriptionInfo>> subscriptions) throws Exception {

	ServerMessage response = new ServerMessage(status, fileContents, subscriptions);

	output.writeObject(response);
	output.flush();

	logger.debug(String.format("Server sent response to server with status = %s", response.getStatus()));

	return;

    }

    public static ServerMessage receiveMessage(ObjectInputStream input) throws Exception {

	ServerMessage request = (ServerMessage) input.readObject();

	logger.debug(String.format("Server received request from server with status = %s", request.getStatus()));

	return request;
    }

}
