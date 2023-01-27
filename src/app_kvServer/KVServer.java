package app_kvServer;

import java.io.*;
import java.net.*;

import org.apache.log4j.Logger;

public class KVServer extends Thread implements IKVServer {

    private static Logger logger = Logger.getRootLogger();
    private ServerSocket socket;

    int port;
    int cacheSize;
    String strategy;

    boolean online;

    /**
	* Start KV Server at given port
	* @param port given port for storage server to operate
	* @param cacheSize specifies how many key-value pairs the server is allowed
	*           to keep in-memory
	* @param strategy specifies the cache replacement strategy in case the cache
	*           is full and there is a GET- or PUT-request on a key that is
	*           currently not contained in the cache. Options are "FIFO", "LRU",
	*           and "LFU".
	*/
    public KVServer(int port, int cacheSize, String strategy) {
	this.port = port;
	this.cacheSize = cacheSize;
	this.strategy = strategy;

	this.online = true;
    }
    
    @Override
    public int getPort() {
	return this.port;
    }

    @Override
    public String getHostname(){
	return this.socket.getInetAddress().getHostName();
    }

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		return "";
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
	}

    @Override
    public void kill() {
	System.exit(1);
    }

    @Override
    public void close() {
	try {
	    this.socket.close();
	} catch (Exception e) {
	    logger.error(e.toString());
	}
	this.online = false;
    }

    @Override
    public void run() {

	logger.info("Starting server...");	
	try {
	    this.socket = new ServerSocket(port);
	    logger.info("Server listening on port: " + this.socket.getLocalPort());
	} catch (IOException e) {
	    logger.error("Cannot open server socket: " + e.toString());
	    return;
	}

	while (this.online) {

	    try {
		Socket client = this.socket.accept();
	    new Connection(client, this).start();
		logger.info(String.format("Connected to %s on port %d", client.getInetAddress().getHostName(), client.getPort()));
	    } catch (IOException e) {
		logger.error(String.format("Unable to establish connection: %s", e.toString()));
	    }

	}

	logger.info("Server stopped...");
    }
}
