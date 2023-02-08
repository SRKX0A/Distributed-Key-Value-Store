package app_kvServer;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Calendar;
import java.util.Scanner;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import shared.messages.KVMessage.StatusType;

public class KVServer extends Thread implements IKVServer {

    private static Logger logger = Logger.getRootLogger();
    private ServerSocket socket;

    private String address;
    private int port;
    private String directory;

    private int cacheSize;

    private volatile boolean online;
    private volatile boolean finished;

    private File wal;
    private TreeMap<String, String> memtable;
    private Object memtableLock;

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
    public KVServer(String address, int port, String directory, int cacheSize) throws IOException {
	this.address = address;
	this.port = port;
	this.directory = directory;
	this.cacheSize = cacheSize;

	this.wal = new File(this.directory, "wal.txt");
	this.memtable = new TreeMap<String, String>();
	this.memtableLock = new Object();

	Scanner scanner = new Scanner(this.wal);
	scanner.useDelimiter("\r\n");
	while (scanner.hasNext()) {
	    String test_key = scanner.next();
	    String test_value = scanner.next();
	    this.memtable.put(test_key, test_value);
	}
	scanner.close();


    }

    @Override
    public int getPort() {
	return this.socket.getLocalPort();
    }

    @Override
    public String getHostname(){
	return this.socket.getInetAddress().getHostName();
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
		return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
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

		    synchronized (this.memtableLock) {
			this.memtable.put(test_key, test_value);
			if (this.memtable.size() >= this.cacheSize) {
			    this.dumpCacheToDisk();
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
		this.dumpCacheToDisk();
	    }
	}

	return response;

    }

    public void dumpCacheToDisk() throws Exception {

	logger.info("Dumping current cache contents to disk.");

	String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());

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

	logger.info("Compacting logs.");

	HashSet<String> keySet = new HashSet<String>();

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

	String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
	File dumpedFile = new File(this.directory, "CompactedKVServerStoreFile_" + timestamp + ".txt"); 
	dumpedFile.createNewFile();
	BufferedWriter dumpedWriter = new BufferedWriter(new FileWriter(dumpedFile, true));
	int keyCount = 0;

	for (File file: store_files) {
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
			timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
			dumpedFile = new File(this.directory, "CompactedKVServerStoreFile_" + timestamp + ".txt"); 
			dumpedFile.createNewFile();
			dumpedWriter = new BufferedWriter(new FileWriter(dumpedFile, true));
			keyCount = 0;
		    }

		}
	    }
	    scanner.close();
	}

    }

    public void clearOldLogs() throws Exception {

	logger.info("Clearing old logs.");

	File store_dir = new File(this.directory);

	File[] store_files = store_dir.listFiles(new FilenameFilter() {
	    public boolean accept(File dir, String name) {
		return name.startsWith("KVServerStoreFile_");
	    }
	});

	for (File file: store_files) {
	    file.delete();
	}

	File[] compacted_files = store_dir.listFiles(new FilenameFilter() {
	    public boolean accept(File dir, String name) {
		return name.startsWith("CompactedKVServerStoreFile_");
	    }
	});

	for (File file: compacted_files) {
	    String filename = file.getName();
	    File newFile = new File(filename.substring(0, 9));
	    file.renameTo(newFile);
	}

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
	try {
	    this.socket.close();
	} catch (Exception e) {
	    logger.error(e.toString());
	}
	this.online = false;
    }

    public boolean isOnline() {
	return this.online;
    }

    public boolean isFinished() {
	return this.finished;
    }

    @Override
    public void run() {

	logger.info("Starting server...");	
	try {
	    this.socket = new ServerSocket(this.port, 0, InetAddress.getByName(this.address));
	    this.online = true;
	    logger.info("Server listening on port: " + this.socket.getLocalPort());
	} catch (IOException e) {
	    logger.error("Cannot open server socket: " + e.toString());
	    this.finished = true;
	    return;
	}

	while (this.online) {

	    try {
		Socket client = this.socket.accept();
		new Connection(client, this).start();
		logger.info(String.format("Connected to %s on port %d", client.getInetAddress().getHostName(), client.getPort()));
	    } catch (SocketException e) {
		logger.info(String.format("SocketException received: %s", e.toString()));
	    } catch (IOException e) {
		logger.error(String.format("Unable to establish connection: %s", e.toString()));
	    }

	}

	this.finished = true;
	logger.info("Server stopped...");
    }
}
