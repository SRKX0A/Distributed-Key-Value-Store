package app_kvServer;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;
import java.time.*;

import org.apache.log4j.Logger;

import client.ProtocolMessage;
import shared.KeyRange;
import shared.ByteArrayComparator;
import shared.messages.KVMessage.StatusType;
import shared.messages.ECSMessage;


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

    private String directory;
    private int cacheSize;
    private int dumpCounter;

    private volatile boolean online;

    private File wal;
    private TreeMap<String, String> memtable;
    private Object memtableLock;

    private volatile TreeMap<byte[], KeyRange> metadata;

    private Timer replicationTimer;

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
    public KVServer(String address, int port, String bootstrapAddress, int bootstrapPort, String directory, int cacheSize) throws IOException {

        this.state = ServerState.SERVER_INITIALIZING;

	this.ecsAddress = bootstrapAddress;
	this.ecsPort = bootstrapPort;

        this.directory = directory;
        this.cacheSize = cacheSize;
	this.dumpCounter = 0;

        this.wal = new File(this.directory, "wal.txt");
        this.memtable = new TreeMap<String, String>();
        this.memtableLock = new Object();

	this.metadata = new TreeMap<byte[], KeyRange>(new ByteArrayComparator());
	this.replicationTimer = new Timer("Replication Timer");

        Scanner scanner = new Scanner(this.wal);
        scanner.useDelimiter("\r\n");
        while (scanner.hasNext()) {
            String test_key = scanner.next();
            String test_value = scanner.next();
            this.memtable.put(test_key, test_value);
        }
        scanner.close();

	logger.info("Starting server...");	
	this.clientSocket = new ServerSocket(port, 0, InetAddress.getByName(address));
	logger.info("Server listening on port: " + this.clientSocket.getLocalPort());

    }

    @Override
    public int getPort() {
        return this.clientSocket.getLocalPort();
    }

    @Override
    public String getHostname(){
        return this.clientSocket.getInetAddress().getHostName();
    }

    public ServerState getServerState() {
        return this.state;
    }

    public void setServerState(ServerState state) {
	this.state = state;
    }

    public TreeMap<byte[], KeyRange> getMetadata() {
	return this.metadata;
    }

    public void setMetadata(TreeMap<byte[], KeyRange> metadata) {
	this.metadata = metadata;
    }

    public void printMetadata() {
	
	System.out.println("METADATA BEGIN");
	for (var entry: this.metadata.entrySet()) {
	    var ringPosition = entry.getKey();
	    var nodeRange = entry.getValue();

	    System.out.print("Key: ");
	    for (var b: ringPosition) {
		System.out.print(String.format("%x", b));
	    }
	    System.out.print(String.format(", Addr+Port: <%s:%d>, RangeFrom: ", nodeRange.getAddress(), nodeRange.getPort()));

	    for (var b: nodeRange.getRangeFrom()) {
		System.out.print(String.format("%x", b));
	    }
	    System.out.print(", RangeTo: ");
	    for (var b: nodeRange.getRangeTo()) {
		System.out.print(String.format("%x", b));
	    }
	    System.out.println();

	}
	System.out.println("METADATA END");

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

        File store_dir = new File(this.directory);

        File[] store_files = store_dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("KVServerStoreFile_");
            }
        });

        Arrays.sort(store_files, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return Long.valueOf(f2.lastModified()).compareTo(f1.lastModified());
            }
        });

        for (File file: store_files) {
            Scanner scanner = new Scanner(file);
            scanner.useDelimiter("\r\n");
            while (scanner.hasNext()) {
                String test_key = scanner.next();
                String test_value = scanner.next();
                if (key.equals(test_key)) {
                    scanner.close();
                    if (test_value.equals("null")) {
                        return false;
                    } else {
                        return true;
}
                }
            }
            scanner.close();
        }

        return false;

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

        File store_dir = new File(this.directory);

        File[] store_files = store_dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("KVServerStoreFile_");
            }
        });

        Arrays.sort(store_files, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return Long.valueOf(f2.lastModified()).compareTo(f1.lastModified());
            }
        });

        for (File file: store_files) {
            Scanner scanner = new Scanner(file);
            scanner.useDelimiter("\r\n");
            while (scanner.hasNext()) {
                String test_key = scanner.next();
                String test_value = scanner.next();
                if (key.equals(test_key)) {
                    logger.info("Got key = " + key + " from storage with value = " + test_value);

		    if (this.state != ServerState.SERVER_REBALANCING) {
			synchronized (this.memtableLock) {
			    this.memtable.put(test_key, test_value);
			    if (this.memtable.size() >= this.cacheSize) {
				this.dumpCounter++;
				this.dumpCacheToDisk();
				if (this.dumpCounter == 3) {
				    this.compactLogs();
				    this.clearOldLogs();
				    this.dumpCounter = 0;
				}
			    }
			}
		    }

                    scanner.close();
                    return test_value;
                }
            }
            scanner.close();
        }

        return "null";

    }

    @Override
    public synchronized StatusType putKV(String key, String value) throws Exception {

        BufferedWriter walWriter = new BufferedWriter(new FileWriter(this.wal, true));
        walWriter.write(String.format("%s\r\n%s\r\n", key, value));
        walWriter.close();

        StatusType response = StatusType.PUT_SUCCESS;

        synchronized (this.memtableLock) {
            boolean presentInCache = this.inCache(key);
            String previousValue = this.memtable.put(key, value);

        if (value.equals("null")) {
                response = StatusType.PUT_SUCCESS;
            } else if ((previousValue != null && !previousValue.equals("null")) || (!presentInCache && this.inStorage(key))) {
                response = StatusType.PUT_UPDATE;
            }

        if (this.memtable.size() >= this.cacheSize) {
		this.dumpCounter++;
		this.dumpCacheToDisk();
		if (this.dumpCounter == 3) {
		    this.compactLogs();
		    this.clearOldLogs();
		    this.dumpCounter = 0;
		}
            }
        }

        return response;

    }

    public void dumpCacheToDisk() throws Exception {

        logger.info("Dumping current cache contents to disk.");

        String timestamp = Instant.now().toString();
        File dumpedFile = new File(this.directory, "KVServerStoreFile_" + timestamp + ".txt"); 
        dumpedFile.createNewFile();

        BufferedWriter dumpedWriter = new BufferedWriter(new FileWriter(dumpedFile, true));
        for (Map.Entry<String, String> entry: this.memtable.entrySet()) {
            dumpedWriter.write(String.format("%s\r\n%s\r\n", entry.getKey(), entry.getValue()));		
        }

        dumpedWriter.close();

        new FileOutputStream(this.wal).close();

        this.memtable.clear();
    }

    public void compactLogs() throws Exception {

        logger.info("Compacting logs");

        HashSet<String> keySet = new HashSet<String>();

        File storeDir = new File(this.directory);

        File[] storeFiles = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("KVServerStoreFile_");
            }
        });

        Arrays.sort(storeFiles, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return Long.valueOf(f2.lastModified()).compareTo(f1.lastModified());
            }
        });

	File dumpedFile = new File(this.directory, "CompactedKVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	dumpedFile.createNewFile();
	BufferedWriter dumpedWriter = new BufferedWriter(new FileWriter(dumpedFile, true));
	int keyCount = 0;

        for (File file: storeFiles) {

            Scanner scanner = new Scanner(file);
            scanner.useDelimiter("\r\n");

            while (scanner.hasNext()) {
                String test_key = scanner.next();
                String test_value = scanner.next();

                if (!keySet.contains(test_key)) {
                    keySet.add(test_key); 
                    dumpedWriter.write(String.format("%s\r\n%s\r\n", test_key, test_value));
		    keyCount++;

		    if (keyCount >= this.cacheSize) {
			dumpedWriter.close();
			dumpedFile = new File(this.directory, "CompactedKVServerStoreFile_" + Instant.now().toString() + ".txt"); 
			dumpedFile.createNewFile();
			dumpedWriter = new BufferedWriter(new FileWriter(dumpedFile, true));
			keyCount = 0;
		    }

                }
            }
            scanner.close();
        }

	dumpedWriter.close();

        File[] compactedFiles = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("CompactedKVServerStoreFile_");
            }
        });

        for (File file: compactedFiles) {
	    if (file.length() == 0) {
		file.delete();
	    }
	}
    }

    public void clearOldLogs() {

        logger.info("Clearing old logs");

        File storeDir = new File(this.directory);

        File[] storeFiles = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("KVServerStoreFile_");
            }
        });

        for (File file: storeFiles) {
            file.delete();
        }

        File[] compacted_files = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("CompactedKVServerStoreFile_");
            }
        });

        for (File file: compacted_files) {
            String filename = file.getName();
            File newFile = new File(this.directory, filename.substring(9));
            file.renameTo(newFile);
        }

    }

    public void partitionLogsByKeyRange() throws Exception {
        
	byte[] serverHash = this.hashIP(this.getHostname(), this.getPort());
	KeyRange serverRange = this.metadata.get(serverHash);

        File storeDir = new File(this.directory);

        File[] storeFiles = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("KVServerStoreFile_");
            }
        });

        File filteredFile = new File(this.directory, "FilteredKVServerStoreFile_" + Instant.now().toString() + ".txt"); 
        filteredFile.createNewFile();
        BufferedWriter filteredWriter = new BufferedWriter(new FileWriter(filteredFile, true));
        int filteredKeyCount = 0;

        File stayFile = new File(this.directory, "KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
        stayFile.createNewFile();
        BufferedWriter stayWriter = new BufferedWriter(new FileWriter(stayFile, true));
        int stayKeyCount = 0;

        for (File file: storeFiles) {
            Scanner scanner = new Scanner(file);
            scanner.useDelimiter("\r\n");
            while (scanner.hasNext()) {
                String test_key = scanner.next();
                String test_value = scanner.next();

                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(test_key.getBytes());
                byte[] keyDigest = md.digest();

                if (serverRange == null || !serverRange.withinKeyRange(keyDigest)) {

                    filteredWriter.write(String.format("%s\r\n%s\r\n", test_key, test_value));		
                    filteredKeyCount++;

                    if (filteredKeyCount >= this.cacheSize) {
                        filteredWriter.close();	
                        filteredFile = new File(this.directory, "FilteredKVServerStoreFile_" + Instant.now().toString() + ".txt"); 
                        filteredFile.createNewFile();
                        filteredWriter = new BufferedWriter(new FileWriter(filteredFile, true));
                        filteredKeyCount = 0;
                    }

                } else {

                    stayWriter.write(String.format("%s\r\n%s\r\n", test_key, test_value));		
                    stayKeyCount++;

                    if (stayKeyCount >= this.cacheSize) {
                        stayWriter.close();	
                        stayFile = new File(this.directory, "KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
                        stayFile.createNewFile();
                        stayWriter = new BufferedWriter(new FileWriter(stayFile, true));
                        stayKeyCount = 0;
                    }
		}
            }
	    file.delete();
            scanner.close();
        }
	
	filteredWriter.close();
	stayWriter.close();

        File[] partitionedFiles = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("KVServerStoreFile_") || name.startsWith("FilteredKVServerStoreFile_");
            }
        });

        for (File file: partitionedFiles) {
	    if (file.length() == 0) {
		file.delete();
	    }
	}

    }

    public void clearFilteredLogs() {

        logger.info("Clearing filtered logs");

        File storeDir = new File(this.directory);

        File[] filteredFiles = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("FilteredKVServerStoreFile_");
            }
        });

        for (File file: filteredFiles) {
            file.delete();
        }

    }

    public void sendAllLogsToServer(String address, int port) throws Exception {

	logger.info(String.format("Sending logs to <%s,%d>", address, port));
	
	Socket serverSocket = new Socket(address, port);
	OutputStream output = serverSocket.getOutputStream();

	ProtocolMessage initialMessage = new ProtocolMessage(StatusType.SERVER_INIT, null, null);
	output.write(initialMessage.getBytes());
	output.flush();


        File storeDir = new File(this.directory);

        File[] filteredFiles = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("FilteredKVServerStoreFile_");
            }
        });

	for (File file: filteredFiles) {
            Scanner scanner = new Scanner(file);
            scanner.useDelimiter("\r\n");
            while (scanner.hasNext()) {
                String key = scanner.next();
                String value = scanner.next();

		ProtocolMessage message = new ProtocolMessage(StatusType.SEND_KV, key, value);

		logger.debug(String.format("Sending key = %s, value = %s", key, value));

		output.write(message.getBytes());
		output.flush();

	    }
	    scanner.close();
	}

	ProtocolMessage finishMessage = new ProtocolMessage(StatusType.SEND_KV_FIN, null, null);
	output.write(finishMessage.getBytes());
	output.flush();


        File[] replica1Files = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("Replica1KVServerStoreFile_");
            }
        });

	for (File file: replica1Files) {
            Scanner scanner = new Scanner(file);
            scanner.useDelimiter("\r\n");
            while (scanner.hasNext()) {
                String key = scanner.next();
                String value = scanner.next();

		ProtocolMessage message = new ProtocolMessage(StatusType.SEND_REPLICA_KV_1, key, value);

		logger.debug(String.format("Sending replica1 key = %s, value = %s", key, value));

		output.write(message.getBytes());
		output.flush();

	    }
	    scanner.close();
	}

	finishMessage = new ProtocolMessage(StatusType.SEND_REPLICA_KV_1_FIN, null, null);
	output.write(finishMessage.getBytes());
	output.flush();


        File[] replica2Files = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("Replica2KVServerStoreFile_");
            }
        });

	for (File file: replica2Files) {
            Scanner scanner = new Scanner(file);
            scanner.useDelimiter("\r\n");
            while (scanner.hasNext()) {
                String key = scanner.next();
                String value = scanner.next();

		ProtocolMessage message = new ProtocolMessage(StatusType.SEND_REPLICA_KV_2, key, value);

		logger.debug(String.format("Sending replica2 key = %s, value = %s", key, value));

		output.write(message.getBytes());
		output.flush();

	    }
	    scanner.close();
	}

	finishMessage = new ProtocolMessage(StatusType.SEND_REPLICA_KV_2_FIN, null, null);
	output.write(finishMessage.getBytes());
	output.flush();
	
	serverSocket.close();

    }

    public void receiveAllLogsFromServer(InputStream input, OutputStream output, ProtocolMessage initialMessage) throws Exception {

	logger.info("Receiving logs from server");

	File receivedFile = new File(this.directory, "KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	receivedFile.createNewFile();
	BufferedWriter receivedWriter = new BufferedWriter(new FileWriter(receivedFile, true));
	int keyCount = 0;

	ProtocolMessage message = null;

	while (true) {

	    try {
		message = Connection.receiveMessage(input);
		if (message.getStatus() == StatusType.SEND_KV_FIN) {
		    receivedWriter.close();
		    break;
		}
	    } catch (Exception e) {
		receivedWriter.close();
		throw e;
	    }

	    String key = message.getKey();
	    String value = message.getValue();
	    
	    receivedWriter.write(String.format("%s\r\n%s\r\n", key, value));
	    keyCount++;

	    if (keyCount >= this.cacheSize) {
		receivedWriter.close();
		receivedFile = new File(this.directory, "KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
		receivedFile.createNewFile();
		receivedWriter = new BufferedWriter(new FileWriter(receivedFile, true));
		keyCount = 0;
	    }

	}

	File receivedReplica1File = new File(this.directory, "Replica1KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	receivedReplica1File.createNewFile();
	BufferedWriter receivedReplica1Writer = new BufferedWriter(new FileWriter(receivedReplica1File, true));
	int receivedReplica1keyCount = 0;

	while (true) {

	    try {
		message = Connection.receiveMessage(input);
		if (message.getStatus() == StatusType.SEND_REPLICA_KV_1_FIN) {
		    receivedReplica1Writer.close();
		    break;
		}
	    } catch (Exception e) {
		receivedReplica1Writer.close();
		throw e;
	    }

	    String key = message.getKey();
	    String value = message.getValue();
	    
	    receivedReplica1Writer.write(String.format("%s\r\n%s\r\n", key, value));
	    receivedReplica1keyCount++;

	    if (receivedReplica1keyCount >= this.cacheSize) {
		receivedReplica1Writer.close();
		receivedReplica1File = new File(this.directory, "Replica1KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
		receivedReplica1File.createNewFile();
		receivedReplica1Writer = new BufferedWriter(new FileWriter(receivedReplica1File, true));
		receivedReplica1keyCount = 0;
	    }

	}

	File receivedReplica2File = new File(this.directory, "Replica2KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	receivedReplica2File.createNewFile();
	BufferedWriter receivedReplica2Writer = new BufferedWriter(new FileWriter(receivedReplica2File, true));
	int receivedReplica2keyCount = 0;

	while (true) {

	    try {
		message = Connection.receiveMessage(input);
		if (message.getStatus() == StatusType.SEND_REPLICA_KV_2_FIN) {
		    receivedReplica2Writer.close();
		    break;
		}
	    } catch (Exception e) {
		receivedReplica2Writer.close();
		throw e;
	    }

	    String key = message.getKey();
	    String value = message.getValue();
	    
	    receivedReplica2Writer.write(String.format("%s\r\n%s\r\n", key, value));
	    receivedReplica2keyCount++;

	    if (receivedReplica2keyCount >= this.cacheSize) {
		receivedReplica2Writer.close();
		receivedReplica2File = new File(this.directory, "Replica2KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
		receivedReplica2File.createNewFile();
		receivedReplica2Writer = new BufferedWriter(new FileWriter(receivedReplica2File, true));
		receivedReplica2keyCount = 0;
	    }

	}

	this.ecsConnection.initializationFinished();

    }

    public void sendReplicatedLogsToServer(StatusType status, String address, int port) throws Exception {

	logger.info(String.format("Sending replicated logs to <%s,%d>", address, port));
	
	Socket serverSocket = new Socket(address, port);
	InputStream input = serverSocket.getInputStream();
	OutputStream output = serverSocket.getOutputStream();

        File storeDir = new File(this.directory);

        File[] storeFiles = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("KVServerStoreFile_");
            }
        });

	ProtocolMessage initialMessage = new ProtocolMessage(StatusType.REPLICATE_KV_HANDSHAKE, this.getKeyRangeSuccessString(), null);
	output.write(initialMessage.getBytes());
	output.flush();

	ProtocolMessage reply = Connection.receiveMessage(input);

	if (reply.getStatus() == StatusType.REPLICATE_KV_HANDSHAKE_ACK) {
	    for (File file: storeFiles) {
		Scanner scanner = new Scanner(file);
		scanner.useDelimiter("\r\n");
		while (scanner.hasNext()) {
		    String key = scanner.next();
		    String value = scanner.next();

		    ProtocolMessage message = new ProtocolMessage(status, key, value);

		    logger.debug(String.format("Replicating key = %s, value = %s", key, value));

		    output.write(message.getBytes());
		    output.flush();

		}
		scanner.close();
	    }

	    ProtocolMessage finishMessage = new ProtocolMessage(StatusType.REPLICATE_KV_FIN, null, null);
	    output.write(finishMessage.getBytes());
	    output.flush();
	}
	
	serverSocket.close();

    }

    public void receiveReplicatedLogsFromServer(InputStream input, OutputStream output, ProtocolMessage initialMessage) throws Exception {

	String currentTopology = this.getKeyRangeSuccessString();
	String senderTopology = initialMessage.getKey();

	if (this.getServerState() != KVServer.ServerState.SERVER_INITIALIZING && !currentTopology.equals(senderTopology)) {
	    logger.warn("Replication request denied due to differing topology");
	    ProtocolMessage finishMessage = new ProtocolMessage(StatusType.REPLICATE_KV_HANDSHAKE_NACK, null, null);
	    output.write(finishMessage.getBytes());
	    output.flush();
	    return;
	} else {
	    ProtocolMessage startMessage = new ProtocolMessage(StatusType.REPLICATE_KV_HANDSHAKE_ACK, null, null);
	    output.write(startMessage.getBytes());
	    output.flush();
	}

	ProtocolMessage message = Connection.receiveMessage(input);	

	if (message.getStatus() == StatusType.REPLICATE_KV_FIN) {
	    return;
	}

	logger.info("Receiving " + message.getStatus().toString() + " messages from server");

	String replicaPrefix = null;
	StatusType replicaType = message.getStatus();

	if (message.getStatus() == StatusType.REPLICATE_KV_1) {
	    replicaPrefix = "Replica1";
	} else if (message.getStatus() == StatusType.REPLICATE_KV_2) {
	    replicaPrefix = "Replica2";
	}

	File replicatedFile = new File(this.directory, "New" + replicaPrefix + "KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	replicatedFile.createNewFile();
	BufferedWriter replicatedWriter = new BufferedWriter(new FileWriter(replicatedFile, true));
	int keyCount = 0;

	while (true) {

	    String key = message.getKey();
	    String value = message.getValue();
	    
	    replicatedWriter.write(String.format("%s\r\n%s\r\n", key, value));
	    keyCount++;

	    if (keyCount >= this.cacheSize) {
		replicatedWriter.close();
		replicatedFile = new File(this.directory, "New" + replicaPrefix + "KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
		replicatedFile.createNewFile();
		replicatedWriter = new BufferedWriter(new FileWriter(replicatedFile, true));
		keyCount = 0;
	    }

	    try {
		message = Connection.receiveMessage(input);
		if (message.getStatus() == StatusType.REPLICATE_KV_FIN) {
		    replicatedWriter.close();
		    break;
		}
	    } catch (Exception e) {
		replicatedWriter.close();
		throw e;
	    }

	}

	this.clearOldReplicatedLogs(replicaType);


    }

    public void clearOldReplicatedLogs(StatusType status) {

	final String replicaPrefix;

	File storeDir = new File(this.directory);

	if (status == StatusType.REPLICATE_KV_1) {
	    logger.info("Clearing old Replica1 logs");
	    replicaPrefix = "Replica1KVServerStoreFile_";
	} else {
	    logger.info("Clearing old Replica2 logs");
	    replicaPrefix = "Replica2KVServerStoreFile_";
	}

	File[] replicatedFiles = storeDir.listFiles(new FilenameFilter() {
	    public boolean accept(File dir, String name) {
		return name.startsWith(replicaPrefix);
	    }
	});

	for (File file: replicatedFiles) {
	    file.delete();
	}

	File[] newReplicatedFiles = storeDir.listFiles(new FilenameFilter() {
	    public boolean accept(File dir, String name) {
		return name.startsWith("New" + replicaPrefix);
	    }
	});

	for (File file: newReplicatedFiles) {
	    String filename = file.getName();
	    File newFile = new File(this.directory, filename.substring(3));
	    file.renameTo(newFile);
	}

	return;

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
	    this.dumpCacheToDisk();
	    this.compactLogs();
	    this.clearOldLogs();
	    this.sendReplicatedLogsToServer(StatusType.REPLICATE_KV_1, firstReplicaKeyRange.getAddress(), firstReplicaKeyRange.getPort());
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
	    this.dumpCacheToDisk();
	    this.compactLogs();
	    this.clearOldLogs();
	    this.sendReplicatedLogsToServer(StatusType.REPLICATE_KV_2, secondReplicaKeyRange.getAddress(), secondReplicaKeyRange.getPort());
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

	logger.info(String.format("Recovering keys from node <%s:%d> shutdown", address, port));

	KeyRange serverKeyRange = updatedMetadata.get(serverRingPosition);

	File recoveredFile = new File(this.directory, "KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
	recoveredFile.createNewFile();
	BufferedWriter recoveredWriter = new BufferedWriter(new FileWriter(recoveredFile, true));
	int recoveredKeyCount = 0;

        File storeDir = new File(this.directory);

        File[] replicatedFiles = storeDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("Replica1KVServerStoreFile_") || name.startsWith("Replica2KVServerStoreFile_");
            }
        });

        Arrays.sort(replicatedFiles, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
            }
        });

	for (File file: replicatedFiles) {
            Scanner scanner = new Scanner(file);
            scanner.useDelimiter("\r\n");
            while (scanner.hasNext()) {
                String test_key = scanner.next();
                String test_value = scanner.next();

                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(test_key.getBytes());
                byte[] keyDigest = md.digest();

                if (serverKeyRange.withinKeyRange(keyDigest)) {

                    recoveredWriter.write(String.format("%s\r\n%s\r\n", test_key, test_value));		
                    recoveredKeyCount++;

                    if (recoveredKeyCount >= this.cacheSize) {
                        recoveredWriter.close();	
                        recoveredFile = new File(this.directory, "KVServerStoreFile_" + Instant.now().toString() + ".txt"); 
                        recoveredFile.createNewFile();
                        recoveredWriter = new BufferedWriter(new FileWriter(recoveredFile, true));
                        recoveredKeyCount = 0;
                    }

                }
            }
            scanner.close();
	}

	recoveredWriter.close();

    }

    public void startReplicationTimer() {
	KVServerReplicationTask replicationTask = new KVServerReplicationTask(this);
	this.replicationTimer.schedule(replicationTask, 1000L, 15000L);
    }

    public void stopReplicationTimer() {
	this.replicationTimer.cancel();
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

        this.online = false;

    }

    public void sendShutdownMessage() {
	if (this.ecsConnection != null) {
	    this.ecsConnection.shutdown();
	}
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
