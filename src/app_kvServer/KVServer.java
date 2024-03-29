package app_kvServer;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;

import org.apache.log4j.Logger;

import app_kvServer.util.ServerFileManager;

import client.ProtocolMessage;
import client.ClientSubscriptionInfo;
import shared.KeyRange;
import shared.ByteArrayComparator;
import shared.messages.ServerMessage;
import shared.messages.ECSMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SubscriptionMessage;

public class KVServer extends Thread implements IKVServer {

    public enum ServerState {
        SERVER_INITIALIZING,
        SERVER_REBALANCING,
        SERVER_AVAILABLE,
        SERVER_UNAVAILABLE
    }

    private static Logger logger = Logger.getRootLogger();

    private ServerState state;

    private ServerSocket clientSocket;

    private ECSConnection ecsConnection;
    private String ecsAddress;
    private int ecsPort;

    private ServerFileManager serverFileManager;

    private TreeMap<String, String> memtable;
    private Object memtableLock;
    private int cacheSize;
    private int dumpCounter;

    private volatile TreeMap<byte[], KeyRange> metadata;

    private Timer replicationTimer;
    private long replicationDelay;

    private volatile boolean online;
	
    private TreeMap<String, List<ClientSubscriptionInfo>> subs;
    private TreeMap<String, List<ClientSubscriptionInfo>> subsReplica;

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
    public KVServer(String address, int port, String bootstrapAddress, int bootstrapPort, String directoryLocation, int cacheSize, long replicationDelay) throws IOException {

        this.state = ServerState.SERVER_INITIALIZING;

	this.ecsAddress = bootstrapAddress;
	this.ecsPort = bootstrapPort;

        this.memtable = new TreeMap<String, String>();
        this.memtableLock = new Object();
        this.cacheSize = cacheSize;
	this.dumpCounter = 0;

	this.serverFileManager = new ServerFileManager(directoryLocation, this.memtable, this.cacheSize);

	this.metadata = new TreeMap<byte[], KeyRange>(new ByteArrayComparator());
	this.replicationTimer = new Timer("Replication Timer");
	this.replicationDelay = replicationDelay;
	
	this.subs = new TreeMap<String, List<ClientSubscriptionInfo>>();

	logger.info("Starting server...");	
	this.clientSocket = new ServerSocket(port, 0, InetAddress.getByName(address));
	logger.info("Server listening on port: " + this.clientSocket.getLocalPort());

    }

    @Override
    public void run() {

	try {
	    this.online = true;
	    this.ecsConnection = new ECSConnection(this);
	    this.ecsConnection.start();
	} catch (Exception e) {
	    logger.error("Could not connect to ECS server: " + e.getMessage());
	    logger.info("Server stopped...");
	    this.close();	
	    return;
	}

        while (this.online) {
            try {
                Socket client = this.clientSocket.accept();
                new Connection(client, this).start();
                logger.info(String.format("Connected to %s on port %d", client.getInetAddress().getHostName(), client.getPort()));
            } catch (SocketException e) {
                logger.info(String.format("SocketException received: %s", e.toString()));
            } catch (IOException e) {
                logger.error(String.format("Unable to establish connection: %s", e.toString()));
            }
        }

        logger.info("Server stopped...");
    }

    @Override
    public int getPort() {
        return this.clientSocket.getLocalPort();
    }

    @Override
    public String getHostname(){
        return this.clientSocket.getInetAddress().getHostName();
    }

    @Override
    public CacheStrategy getCacheStrategy(){
        return IKVServer.CacheStrategy.None;
    }

    @Override
    public int getCacheSize(){
        return this.cacheSize;
    }

    @Override
    public boolean inStorage(String key) throws Exception {
	String result = this.serverFileManager.searchForKeyInFiles(key, "KVServerStoreFile_");
	return !result.equals("null");
    }

    public String getValueFromStorage(String key) throws Exception {
	return this.serverFileManager.searchForKeyInFiles(key, "KVServerStoreFile_");
    }

    @Override
    public boolean inCache(String key) {
        return this.memtable.containsKey(key);
    }

    @Override
    public String getKV(String key) throws Exception {

        if (this.memtable.containsKey(key)) {
            logger.info("Got key = " + key + " from cache with value = " + this.memtable.get(key));
            return this.memtable.get(key);
        }

	String value = this.serverFileManager.searchForKeyInFiles(key, "Replica1KVServerStoreFile_");

	if (!value.equals("null")) {
	    logger.info("Got key = " + key + " from replica 1 storage with value = " + value);
	    return value;
	}

	value = this.serverFileManager.searchForKeyInFiles(key, "Replica2KVServerStoreFile_");

	if (!value.equals("null")) {
	    logger.info("Got key = " + key + " from replica 2 storage with value = " + value);
	    return value;
	}

	value = this.serverFileManager.searchForKeyInFiles(key, "KVServerStoreFile_");

	if (!value.equals("null")) {
	    logger.info("Got key = " + key + " from storage with value = " + value);

	    if (this.state != ServerState.SERVER_REBALANCING) {
		synchronized (this.memtableLock) {
		    this.memtable.put(key, value);
		    if (this.memtable.size() >= this.cacheSize) {
			this.dumpCounter++;
			this.serverFileManager.dumpCacheToStoreFile();
			if (this.dumpCounter == 3) {
			    this.serverFileManager.compactStoreFiles();
			    this.serverFileManager.clearOldStoreFiles();
			    this.dumpCounter = 0;
			}
		    }
		}
	    }

	    return value;
	}

        return "null";

    }

    @Override
    public synchronized StatusType putKV(String key, String value) throws Exception {

	this.serverFileManager.writeKVToWAL(key, value);

        StatusType response = StatusType.PUT_SUCCESS;
	String previousValue;

	synchronized (this.memtableLock) {

	    previousValue = this.memtable.put(key, value);

	    if (previousValue != null && !previousValue.equals("null")) {
		response = StatusType.PUT_UPDATE;
	    }

	    if (response != StatusType.PUT_UPDATE) {
		previousValue = this.getValueFromStorage(key);

		if (previousValue != null && !previousValue.equals("null")) {
		    response = StatusType.PUT_UPDATE;
		}
	    }

	    if (value.equals("null")) {
		response = StatusType.PUT_SUCCESS;
	    }

	    if (this.memtable.size() >= this.cacheSize) {
		this.dumpCounter++;
		this.serverFileManager.dumpCacheToStoreFile();
		if (this.dumpCounter == 3) {
		    this.serverFileManager.compactStoreFiles();
		    this.serverFileManager.clearOldStoreFiles();
		    this.dumpCounter = 0;
		}
	    }
	}
	
	this.notifyClients(key, value, previousValue);

	return response;

    }

    @Override
    public void clearCache() {
        this.memtable.clear();
    }

    @Override
    public void clearStorage() {
        return;	
    }

    @Override
    public void kill() {
        System.exit(1);
    }

    @Override
    public void close() {

	this.replicationTimer.cancel();

        this.online = false;

        try {

	    if (this.clientSocket != null) {
		this.clientSocket.close();
		this.clientSocket = null;
	    }

        } catch (Exception e) {
            logger.error("Could not gracefully close client socket: " + e.getMessage());
        }

	try {

	    if (this.ecsConnection != null) {
		this.ecsConnection.getSocket().close();
		this.ecsConnection = null;
	    }

	} catch (Exception e) {
	    logger.error("Could not gracefully close ECS socket: " + e.getMessage());
	}

    }

    public void sendAllFilesToServer(String address, int port) throws Exception {

	logger.info(String.format("Sending all files to <%s,%d>", address, port));
	
	synchronized (this.memtableLock) {
	    this.serverFileManager.dumpCacheToStoreFile();
	    this.serverFileManager.compactStoreFiles();
	    this.serverFileManager.clearOldStoreFiles();
	}

	byte[] serverHash = this.hashIP(this.getHostname(), this.getPort());
	KeyRange serverKeyRange = this.metadata.get(serverHash);

	this.serverFileManager.partitionStoreFilesByKeyRange(serverHash, serverKeyRange);

	Socket serverSocket = new Socket(address, port);
	OutputStream output = serverSocket.getOutputStream();

	ProtocolMessage initialMessage = new ProtocolMessage(StatusType.SERVER_INIT, null, null);
	output.write(initialMessage.getBytes());
	output.flush();

	File[] partitionedFiles = this.serverFileManager.filterFilesByPrefix("PartitionedKVServerStoreFile_");

	ObjectOutputStream oos = new ObjectOutputStream(output);

	for (File file: partitionedFiles) {
	    ServerConnection.sendMessage(oos, ServerMessage.StatusType.SEND_KV, this.serverFileManager.fileTofileContentsMatrix(file), null);
	}

	File[] replica1Files = this.serverFileManager.filterFilesByPrefix("Replica1KVServerStoreFile_");

	for (File file: replica1Files) {
	    ServerConnection.sendMessage(oos, ServerMessage.StatusType.SEND_REPLICA_KV_1, this.serverFileManager.fileTofileContentsMatrix(file), null);
	}

	File[] replica2Files = this.serverFileManager.filterFilesByPrefix("Replica2KVServerStoreFile_");

	for (File file: replica2Files) {
	    ServerConnection.sendMessage(oos, ServerMessage.StatusType.SEND_REPLICA_KV_2, this.serverFileManager.fileTofileContentsMatrix(file), null);
	}

	var newSubscriptions = this.partitionSubscriptionsForNewServer(address, port);
	ServerConnection.sendMessage(oos, ServerMessage.StatusType.SEND_SUBSCRIPTIONS, null, newSubscriptions);

	ServerConnection.sendMessage(oos, ServerMessage.StatusType.SERVER_INIT_FIN, null, null);

	serverSocket.shutdownOutput();
	serverSocket.close();

	this.serverFileManager.clearPartitionedFiles();

    }

    public TreeMap<String, List<ClientSubscriptionInfo>> partitionSubscriptionsForNewServer(String address, int port) {

	var filteredSubscriptions = new TreeMap<String, List<ClientSubscriptionInfo>>();

	var serverRingPosition = this.hashIP(this.getHostname(), this.getPort());
	var serverKeyRange = this.metadata.get(serverRingPosition);

	synchronized (this.subs) {
	    var subscriptionIterator = this.subs.entrySet().iterator();
	    while (subscriptionIterator.hasNext()) {

		var subscriptionEntry = subscriptionIterator.next();
		var keyPosition = this.hashKey(subscriptionEntry.getKey());	

		if (!serverKeyRange.withinKeyRange(keyPosition)) {
		    filteredSubscriptions.put(subscriptionEntry.getKey(), subscriptionEntry.getValue());
		    subscriptionIterator.remove();
		}
	    }
	}

	return filteredSubscriptions;	

    }

    public void sendFilesToReplicaServer(ServerMessage.StatusType status, String address, int port) throws Exception {

	logger.info(String.format("Sending files to replica server <%s,%d>", address, port));
	
	Socket serverSocket = new Socket(address, port);
	InputStream input = serverSocket.getInputStream();
	OutputStream output = serverSocket.getOutputStream();

	ProtocolMessage initialMessage = new ProtocolMessage(StatusType.REPLICATE_KV_HANDSHAKE, this.getKeyRangeSuccessString(), null);
	output.write(initialMessage.getBytes());
	output.flush();

	File[] storeFiles = this.serverFileManager.filterFilesByPrefix("KVServerStoreFile_");

	ObjectOutputStream oos = new ObjectOutputStream(output);

	ProtocolMessage reply = Connection.receiveMessage(input);

	if (reply.getStatus() == StatusType.REPLICATE_KV_HANDSHAKE_ACK) {

	    for (File file: storeFiles) {
		ServerConnection.sendMessage(oos, status, this.serverFileManager.fileTofileContentsMatrix(file), null);
	    }

	    ServerConnection.sendMessage(oos, ServerMessage.StatusType.REPLICATE_SUBSCRIPTIONS, null, new TreeMap<String, List<ClientSubscriptionInfo>>(this.subs));

	    if (status == ServerMessage.StatusType.REPLICATE_KV_1) {
		ServerConnection.sendMessage(oos, ServerMessage.StatusType.REPLICATE_KV_1_FIN, null, null);
	    } else {
		ServerConnection.sendMessage(oos, ServerMessage.StatusType.REPLICATE_KV_2_FIN, null, null);
	    }
	
	}

	serverSocket.shutdownOutput();
	serverSocket.close();

    }

    public synchronized void replicate() {

	var serverRingPosition = this.hashIP(this.getHostname(), this.getPort());
	var serverKeyRange = this.metadata.get(serverRingPosition);

	var firstReplica = this.metadata.higherEntry(serverRingPosition);

	if (firstReplica == null) {
	    firstReplica = this.metadata.firstEntry();
	}

	var firstReplicaKeyRange = firstReplica.getValue();

	if (serverKeyRange.equals(firstReplicaKeyRange)) {
	    return;
	}

	try {
	    synchronized (this.memtableLock) {
		this.serverFileManager.dumpCacheToStoreFile();
		this.serverFileManager.compactStoreFiles();
		this.serverFileManager.clearOldStoreFiles();
	    }
	    this.sendFilesToReplicaServer(ServerMessage.StatusType.REPLICATE_KV_1, firstReplicaKeyRange.getAddress(), firstReplicaKeyRange.getPort());
	} catch (Exception e) {
	    logger.warn("Failed to complete replication on first replica: " + e.getMessage());
	}

	var secondReplica = metadata.higherEntry(firstReplicaKeyRange.getRangeFrom());

	if (secondReplica == null) {
	    secondReplica = metadata.firstEntry();
	}

	var secondReplicaKeyRange = secondReplica.getValue();

	if (serverKeyRange.equals(secondReplicaKeyRange)) {
	    return;
	}

	try {
	    synchronized (this.memtableLock) {
		this.serverFileManager.dumpCacheToStoreFile();
		this.serverFileManager.compactStoreFiles();
		this.serverFileManager.clearOldStoreFiles();
	    }
	    this.sendFilesToReplicaServer(ServerMessage.StatusType.REPLICATE_KV_2, secondReplicaKeyRange.getAddress(), secondReplicaKeyRange.getPort());
	} catch (Exception e) {
	    logger.warn("Failed to complete replication on second replica: " + e.getMessage());
	}

    }

    public void recoverIfNecessary(ECSMessage message) throws Exception {
	
	String address = message.getAddress();
	int port = message.getPort();

	byte[] serverRingPosition = message.getRingPosition();
	byte[] targetRingPosition = this.hashIP(address, port);

	var updatedMetadata = message.getMetadata();	

	if (updatedMetadata.get(targetRingPosition) != null) {
	    return;
	}

	var responsibleServerRingPosition = updatedMetadata.ceilingKey(targetRingPosition);

	if (responsibleServerRingPosition == null) {
	    responsibleServerRingPosition = updatedMetadata.firstKey();
	}

	if (!Arrays.equals(serverRingPosition, responsibleServerRingPosition)) {
	    return;
	}

	this.serverFileManager.recover(address, port, updatedMetadata, serverRingPosition);

	var serverKeyRange = this.metadata.get(serverRingPosition);

	synchronized (this.subs) {
	    var subscriptionIterator = this.subsReplica.entrySet().iterator();
	    while (subscriptionIterator.hasNext()) {

		var subscriptionEntry = subscriptionIterator.next();
		var keyPosition = this.hashKey(subscriptionEntry.getKey());	

		if (!serverKeyRange.withinKeyRange(keyPosition)) {
		    this.subs.put(subscriptionEntry.getKey(), subscriptionEntry.getValue());
		    subscriptionIterator.remove();
		}
	    }
	}

    }

    public String getKeyRangeSuccessString() {

	StringBuilder sb = new StringBuilder();

	for (var entry: this.metadata.entrySet()) {
	    KeyRange nodeRange = entry.getValue();
	    for (var b: nodeRange.getRangeTo()) {
		sb.append(String.format("%02x", b));
	    }
	    sb.append(",");
	    for (var b: nodeRange.getRangeFrom()) {
		sb.append(String.format("%02x", b));
	    }
	    sb.append(",");
	    sb.append(nodeRange.getAddress());
	    sb.append(":");
	    sb.append(Integer.toString(nodeRange.getPort()));
	    sb.append(";");
	}
	
	return sb.toString();
    }

    public String getKeyRangeReadSuccessString() {
	StringBuilder sb = new StringBuilder();

	for (var entry: this.metadata.entrySet()) {
	    KeyRange primaryKeyRange = entry.getValue();

	    var firstReplicaEntry = this.metadata.lowerEntry(primaryKeyRange.getRangeFrom());

	    if (firstReplicaEntry == null) {
		firstReplicaEntry = this.metadata.lastEntry();
	    }

	    var secondReplicaEntry = this.metadata.lowerEntry(firstReplicaEntry.getKey());

	    if (secondReplicaEntry == null) {
		secondReplicaEntry = this.metadata.lastEntry();
	    }

	    byte[] toPosition = primaryKeyRange.getRangeFrom();
	    byte[] fromPosition = null;

	    if (Arrays.equals(primaryKeyRange.getRangeFrom(), firstReplicaEntry.getValue().getRangeFrom())) {
		fromPosition = primaryKeyRange.getRangeTo();
	    } else if (Arrays.equals(primaryKeyRange.getRangeFrom(), secondReplicaEntry.getValue().getRangeFrom())) {
		fromPosition = firstReplicaEntry.getValue().getRangeTo();
	    } else {
		fromPosition = secondReplicaEntry.getValue().getRangeTo();
	    }

	    for (var b: fromPosition) {
		sb.append(String.format("%02x", b));
	    }
	    sb.append(",");
	    for (var b: toPosition) {
		sb.append(String.format("%02x", b));
	    }
	    sb.append(",");
	    sb.append(primaryKeyRange.getAddress());
	    sb.append(":");
	    sb.append(Integer.toString(primaryKeyRange.getPort()));
	    sb.append(";");
	}
	
	return sb.toString();

    }

    public void startReplicationTimer() {
	KVServerReplicationTask replicationTask = new KVServerReplicationTask(this);
	this.replicationTimer.schedule(replicationTask, 1000L, this.replicationDelay);
    }

    public void stopReplicationTimer() {
	this.replicationTimer.cancel();
    }

    public void sendShutdownMessage() {
	if (this.ecsConnection != null) {
	    this.ecsConnection.shutdown();
	}
    }

    public ServerState getServerState() {
        return this.state;
    }

    public void setServerState(ServerState state) {
	this.state = state;
    }

    public void setSubscriptions(TreeMap<String, List<ClientSubscriptionInfo>> subscriptions) {
	this.subs = subscriptions;
    }

    public TreeMap<byte[], KeyRange> getMetadata() {
	return this.metadata;
    }

    public void setMetadata(TreeMap<byte[], KeyRange> metadata) {
	this.metadata = metadata;
    }

    public boolean isOnline() {
        return this.online;
    }

    public void setOnline(boolean online) {
	this.online = online;
    }

    public String getECSAddress() {
        return this.ecsAddress;
    }

    public int getECSPort() {
        return this.ecsPort;
    }

    public ECSConnection getECSConnection() {
	return this.ecsConnection;
    }

    public ServerFileManager getServerFileManager() {
	return this.serverFileManager;
    }

    public void setSubscriptionsReplica(TreeMap<String, List<ClientSubscriptionInfo>> subscriptions) {
	this.subsReplica = subscriptions;
    }
	
    public boolean subscribeClient(String clientAddress, int clientPort, String key) {
    		
	var clientInfo = new ClientSubscriptionInfo(clientAddress, clientPort);

	logger.info("Client " + clientInfo.toString() + " requests to subscribe to key = " + key);
		
	if (!this.subs.containsKey(key)) {
	    synchronized (this.subs) {
		var cl = new ArrayList<ClientSubscriptionInfo>();
		cl.add(clientInfo);
		subs.put(key, cl);
	    }
	    logger.info("New key " + key + " added to subscription list with client " + clientInfo.toString());
	    return true;
	} 
    	
	var clientList = this.subs.get(key);

	if (clientList.contains(clientInfo)) {
	    logger.debug("Client has already subscribed to key = " + key);
	    return false;
	}

	synchronized (this.subs) {
	    if (!clientList.add(clientInfo)) {
		logger.warn("Client cannot subscribe to key = " + key);
		return false;
	    }
	}
	
	logger.info("Added client " + clientInfo.toString() + " to key = " + key);
		
	return true;

    }

    public boolean unsubscribeClient(String clientAddress, int clientPort, String key) {
    		
	var clientInfo = new ClientSubscriptionInfo(clientAddress, clientPort);

	logger.info("Client " + clientInfo.toString() + " requests to unsubscribe to key = " + key);
    
	if (!this.subs.containsKey(key) || this.subs.get(key).size() == 0) {
	    logger.warn("No clients are subscribed to key = " + key);
	    return false;
	}
	
	synchronized (this.subs) {
	    if (!this.subs.get(key).remove(clientInfo)) {
		logger.warn("Client " + clientInfo.toString() + " is not subsribed to key = " + key);
		return false;
	    }
	}
	
	logger.info("Successfully unsubscribed client " + clientInfo.toString() + " from key = " + key);
	
	return true;
    }
	

    private static void sendNotification(ObjectOutputStream output, String key, String oldValue, String value) {

	SubscriptionMessage notify = new SubscriptionMessage(SubscriptionMessage.StatusType.KV_NOTIFICATION, key, oldValue, value);

	try{	
	    output.writeObject(notify);
	    output.flush();
	} catch(Exception e){
	    logger.error(e.getMessage());
	}

	logger.debug(String.format("Server sent a notification to client with status = %s", notify.getStatus()));

	return;
    }

    private void notifyClients(String key, String value, String oldValue) {
    
	if (!this.subs.containsKey(key)) {
	    return;
	}
	
	logger.info("Notifying subscribers that key = " + key + " with old value = " + oldValue + " now has value = " + value);
	
	synchronized (this.subs) {
	    try {

		List<ClientSubscriptionInfo> clients = this.subs.get(key);

		for(var cli: clients) {
		    String addr = cli.getAddress();
		    int port = cli.getPort();

		    Socket socket = new Socket(addr, port);

		    ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());

		    sendNotification(output, key, oldValue, value);

		    socket.shutdownOutput();
		    socket.close();
		}

	    } catch(Exception e) {
		logger.error(e.getMessage());
	    }
	}
    
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
	try {
	    String valueToHash = key;
	    MessageDigest md = MessageDigest.getInstance("MD5");
	    md.update(valueToHash.getBytes());
	    return md.digest();
	} catch (Exception e) {
	    throw new RuntimeException("Error: Impossible NoSuchAlgorithmError!");
	}
    }

}

class KVServerReplicationTask extends TimerTask {

    private KVServer kvServer;

    public KVServerReplicationTask(KVServer kvServer) {
	this.kvServer = kvServer;
    }

    @Override
    public void run() {
	this.kvServer.replicate();
    }

}
