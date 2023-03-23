package app_kvServer;

import java.io.*;
import java.net.*;
import java.time.*;

import org.apache.log4j.Logger;

import shared.messages.ServerMessage;
import shared.messages.ServerMessage.StatusType;

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
		} else if (request.getStatus() == StatusType.REPLICATE_KV_1_FIN || request.getStatus() == StatusType.REPLICATE_KV_2_FIN) {
		    this.handleReplicateKVFinMessage(request);
		} else if (request.getStatus() == StatusType.SERVER_INIT_FIN) {
		    this.handleServerInitFinMessage();
		    return;
		}

	    }

	} catch (Exception e) {

	}

    }

    public void handleSendKVMessage(ServerMessage request) {

	File newKVFile = new File(this.kvServer.getDirectory(), "KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	try (FileOutputStream fileOutput = new FileOutputStream(newKVFile, true)) {
	    for (byte[] b: request.getFileContents()) {
		fileOutput.write(b); 
	    } 
	} catch (Exception e) {
	    logger.error("Failed to create new KVServerStoreFile: " + e.getMessage()); 
	}

    }

    public void handleReplicaKVMessage(ServerMessage request) {

	File newReplicaFile = null;
	if (request.getStatus() == StatusType.SEND_REPLICA_KV_1) {
	    newReplicaFile = new File(this.kvServer.getDirectory(), "Replica1KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	} else {
	    newReplicaFile = new File(this.kvServer.getDirectory(), "Replica2KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	}

	try (FileOutputStream fileOutput = new FileOutputStream(newReplicaFile, true)) {
	    for (byte[] b: request.getFileContents()) {
		fileOutput.write(b); 
	    } 
	} catch (Exception e) {
	    logger.error("Failed to create new KVServerStoreFile: " + e.getMessage()); 
	}

    }

    public void handleReplicateRequestMessage(ServerMessage request) {

	File newReplicatedFile = null;
	if (request.getStatus() == StatusType.REPLICATE_KV_1) {
	    newReplicatedFile = new File(this.kvServer.getDirectory(), "NewReplica1KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	} else {
	    newReplicatedFile = new File(this.kvServer.getDirectory(), "NewReplica2KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	}

	try (FileOutputStream fileOutput = new FileOutputStream(newReplicatedFile, true)) {
	    for (byte[] b: request.getFileContents()) {
		fileOutput.write(b); 
	    } 
	} catch (Exception e) {
	    logger.error("Failed to create new replicated KVServerStoreFile: " + e.getMessage()); 
	}

	return;

    }

    public void handleReplicateKVFinMessage(ServerMessage request) {
	this.kvServer.clearOldReplicatedLogs(request.getStatus());
    }

    public void handleServerInitFinMessage() {
	this.kvServer.getECSConnection().initializationFinished();
    }

    public static void sendMessage(ObjectOutputStream output, StatusType status, byte[][] fileContents) throws Exception {

	ServerMessage response = new ServerMessage(status, fileContents);

	output.writeObject(response);
	output.flush();

	logger.debug(String.format("Server sent request to Server with status = %s\n", response.getStatus()));

	return;

    }

    public static ServerMessage receiveMessage(ObjectInputStream input) throws Exception {

	ServerMessage request = (ServerMessage) input.readObject();

	return request;
    }

}
