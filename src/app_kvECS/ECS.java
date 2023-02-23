package app_kvECS;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;

import org.apache.log4j.Logger;

import shared.KeyRange;

public class ECS extends Thread {

    private static Logger logger = Logger.getRootLogger();

    private ServerSocket socket;


    private volatile boolean online;
    private volatile boolean finished;

    private int numNodes; 

    private TreeMap<byte[], KeyRange> metadata;
    private HashMap<byte[], ECSClientConnection> connections;

    public ECS(String address, int port) {

	this.numNodes = 0;
	this.metadata = new TreeMap<byte[], KeyRange>();
	this.connections = new HashMap<byte[], ECSClientConnection>();

	logger.info("ECS starting...");

	try {
            this.socket = new ServerSocket(port, 0, InetAddress.getByName(address));
            this.online = true;
            logger.info("Server listening on port: " + this.socket.getLocalPort());
        } catch (IOException e) {
            logger.error("Cannot open client socket: " + e.getMessage());
            this.finished = true;
            return;
	}

    }

    public KeyRange addNode(String address, int port) {

	byte[] ringPosition = hashIP(address, port);

	KeyRange range = null;

	if (this.numNodes == 0) {
	    range = new KeyRange(port, address, ringPosition, ringPosition);
	    this.metadata.put(ringPosition, range);
	} else {

	    byte[] lowerPosition = this.metadata.lowerKey(ringPosition);
	    byte[] upperPosition = this.metadata.higherKey(ringPosition);

	    if (lowerPosition == null) {
		lowerPosition = this.metadata.lastKey();
	    }

	    if (upperPosition == null) {
		upperPosition = this.metadata.firstKey();
	    }

	    KeyRange oldUpperRange = this.metadata.get(upperPosition);
	    KeyRange newUpperRange = new KeyRange(oldUpperRange.getPort(), oldUpperRange.getAddress(), oldUpperRange.getRangeFrom(), ringPosition);
	    range = new KeyRange(port, address, ringPosition, lowerPosition);

	    this.metadata.put(upperPosition, newUpperRange);
	    this.metadata.put(ringPosition, range);
	}

	this.numNodes++;

	return range;

    }

    public KeyRange removeNode(String address, int port) {
	
	byte[] ringPosition = hashIP(address, port);

	if (!this.metadata.containsKey(ringPosition)) {
	    return new KeyRange(0, null, null, null);
	}

	KeyRange range = null;

	if (this.numNodes == 0) {
	    throw new IllegalArgumentException("Error: Cannot call removeNode with no nodes present in ECS");
	} else if (this.numNodes == 1) {
	    range = this.metadata.remove(ringPosition);
	} else {
	    
	    byte[] lowerPosition = this.metadata.lowerKey(ringPosition);
	    byte[] upperPosition = this.metadata.higherKey(ringPosition);

	    if (lowerPosition == null) {
		lowerPosition = this.metadata.lastKey();
	    }

	    if (upperPosition == null) {
		upperPosition = this.metadata.firstKey();
	    }

	    KeyRange oldUpperRange = this.metadata.get(upperPosition);
	    KeyRange newUpperRange = new KeyRange(oldUpperRange.getPort(), oldUpperRange.getAddress(), oldUpperRange.getRangeFrom(), lowerPosition);
	    this.metadata.put(upperPosition, newUpperRange);

	    range = this.metadata.remove(ringPosition);
	}

	this.numNodes--;

	return range;

    }

    public int getNumNodes() {
	return this.numNodes;
    }

    public TreeMap<byte[], KeyRange> getMetadata() {
	return this.metadata;
    }

    public HashMap<byte[], ECSClientConnection> getConnections() {
	return this.connections;
    }

    @Override
    public void run() {
	
	if (this.finished) {
	    return;
	}

        while (this.online) {
            try {
                Socket client = this.socket.accept();
		ECSClientConnection connection = new ECSClientConnection(client, this);
		this.connections.put(this.hashIP(client.getInetAddress().getHostName(), client.getLocalPort()), connection);
		connection.start();
                logger.info(String.format("Connected to %s on port %d", client.getInetAddress().getHostName(), client.getPort()));
            } catch (SocketException e) {
                logger.info(String.format("SocketException received: %s", e.toString()));
            } catch (IOException e) {
                logger.error(String.format("Unable to establish connection: %s", e.toString()));
            }
        }

        this.finished = true;
        logger.info("ECS stopped...");

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
