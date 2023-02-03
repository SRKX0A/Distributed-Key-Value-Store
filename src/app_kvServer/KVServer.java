package app_kvServer;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.Comparator;
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

    private boolean online;

    private File wal;
    private TreeMap<String, String> memtable;

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
    public KVServer(String address, int port, String directory) throws IOException {
	this.address = address;
	this.port = port;
	this.directory = directory;
	this.cacheSize = 5;

	this.online = true;

	this.wal = new File(this.directory, "wal.txt");
	this.memtable = new TreeMap<String, String>();

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
	return this.port;
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
    public boolean inCache(String key){
	return this.memtable.containsKey(key);
    }

    @Override
    public String getKV(String key) throws Exception {

	if (this.memtable.containsKey(key)) {
	    this.logger.info("Got key = " + key + " from cache with value = " + this.memtable.get(key));
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
		    this.logger.info("Got key = " + key + " from storage with value = " + test_value);
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
	
	boolean presentInCache = this.inCache(key);
	String previousValue = this.memtable.put(key, value);

	StatusType response = StatusType.PUT_SUCCESS;

	if ((previousValue != null && !previousValue.equals("null")) || (!presentInCache && this.inStorage(key))) {
	    response = StatusType.PUT_UPDATE;
	}

	if (this.memtable.size() >= this.cacheSize) {

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

	return response;

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
	    this.socket = new ServerSocket(this.port, 0, InetAddress.getByName(this.address));
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
