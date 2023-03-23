package app_kvServer.util;

import java.io.*;
import java.util.*;
import java.security.*;
import java.time.*;

import org.apache.log4j.Logger;

import shared.KeyRange;
import shared.messages.ServerMessage;

public class ServerFileManager {

    private static Logger logger = Logger.getRootLogger();

    private File dataDirectory;
    private File wal;
    private TreeMap<String, String> cache;
    private int cacheSize;

    public ServerFileManager(String dataDirectoryLocation, TreeMap<String, String> cache, int cacheSize) throws IOException {
	this.dataDirectory =  new File(dataDirectoryLocation);
	this.wal = new File(this.dataDirectory, "wal.txt");
	this.cache = cache;
	this.cacheSize = cacheSize;

	try (var scanner = new Scanner(this.wal)) {
	    scanner.useDelimiter("\r\n");
	    while (scanner.hasNext()) {
		String test_key = scanner.next();
		String test_value = scanner.next();
		this.cache.put(test_key, test_value);
	    }
	}
    }

    public void writeKVToWAL(String key, String value) throws Exception {
        BufferedWriter walWriter = new BufferedWriter(new FileWriter(this.wal, true));
        walWriter.write(String.format("%s\r\n%s\r\n", key, value));
        walWriter.close();
    }

    public void dumpCacheToStoreFile() throws Exception {

        logger.info("Dumping current cache contents to disk.");

	try (var dumpedWriter = new LimitedKVBufferedWriter(this.dataDirectory, "KVServerStoreFile_", this.cacheSize)) {
	    for (Map.Entry<String, String> entry: this.cache.entrySet()) {
		dumpedWriter.writeKV(entry.getKey(), entry.getValue());		
	    }
	}

        new FileOutputStream(this.wal).close();
        this.cache.clear();
    }

    public void compactStoreFiles() throws Exception {
	
        logger.info("Compacting KVServerStoreFiles");

        HashSet<String> keySet = new HashSet<String>();

	File[] storeFiles = this.getFilesInReverseChronologicalOrder("KVServerStoreFile_");

	try (var dumpedWriter = new LimitedKVBufferedWriter(this.dataDirectory, "CompactedKVServerStoreFile_", this.cacheSize)) {
	    for (File file: storeFiles) {
		try (var scanner = new Scanner(file)) {
		    scanner.useDelimiter("\r\n");
		    while (scanner.hasNext()) {
			String key = scanner.next();
			String value = scanner.next();

			if (!keySet.contains(key)) {
			    keySet.add(key); 
			    dumpedWriter.writeKV(key, value);
			}
		    }
		}
	    }
	}
    }

    public void clearOldStoreFiles() {

        logger.info("Clearing old KVServerStoreFiles");

	File[] storeFiles = this.filterFilesByPrefix("KVServerStoreFile_");

        for (File file: storeFiles) {
            file.delete();
        }

        File[] compacted_files = this.filterFilesByPrefix("CompactedKVServerStoreFile_");

        for (File file: compacted_files) {
            String filename = file.getName();
            File newFile = new File(this.dataDirectory, filename.substring(9));
            file.renameTo(newFile);
        }

    }

    public void partitionStoreFilesByKeyRange(byte[] serverHash, KeyRange serverRange) throws Exception {
        
	File[] storeFiles = this.filterFilesByPrefix("KVServerStoreFile_");

	try (var leaveWriter = new LimitedKVBufferedWriter(this.dataDirectory, "PartitionedKVServerStoreFile_", this.cacheSize);
	    var stayWriter = new LimitedKVBufferedWriter(this.dataDirectory, "KVServerStoreFile_", this.cacheSize)) {
	    
	    for (File file: storeFiles) {
		try (var scanner = new Scanner(file)) {
		    scanner.useDelimiter("\r\n");
		    while (scanner.hasNext()) {
			String key = scanner.next();
			String value = scanner.next();

			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte[] keyDigest = md.digest();

			if (serverRange == null || !serverRange.withinKeyRange(keyDigest)) {
			    leaveWriter.writeKV(key, value);		
			} else {
			    stayWriter.writeKV(key, value);
			}
		    }
		    file.delete();
		}
	    }
	}
    }

    public void clearPartitionedFiles() {

        logger.info("Clearing PartitionedKVServerStoreFiles");

	File[] partitionedFiles = this.filterFilesByPrefix("PartitionedKVServerStoreFile_");

        for (File file: partitionedFiles) {
            file.delete();
        }

    }

    public void clearOldReplicatedStoreFiles(ServerMessage.StatusType status) {

	final String replicaPrefix;

	if (status == ServerMessage.StatusType.REPLICATE_KV_1_FIN) {
	    logger.info("Clearing old Replica1 logs");
	    replicaPrefix = "Replica1KVServerStoreFile_";
	} else {
	    logger.info("Clearing old Replica2 logs");
	    replicaPrefix = "Replica2KVServerStoreFile_";
	}

	File[] replicatedFiles = this.filterFilesByPrefix(replicaPrefix);

	for (File file: replicatedFiles) {
	    file.delete();
	}

	File[] newReplicatedFiles = this.filterFilesByPrefix("New" + replicaPrefix);

	for (File file: newReplicatedFiles) {
	    String filename = file.getName();
	    File newFile = new File(this.dataDirectory, filename.substring(3));
	    file.renameTo(newFile);
	}

	return;

    }

    public void recover(String address, int port, TreeMap<byte[], KeyRange> metadata, byte[] serverRingPosition) throws Exception {

	logger.info(String.format("Recovering keys from node <%s:%d> shutdown", address, port));

	KeyRange serverKeyRange = metadata.get(serverRingPosition);

	File[] replicatedFiles = this.filterFilesByPrefix("Replica");

	try (var recoveryWriter = new LimitedKVBufferedWriter(this.dataDirectory, "KVServerStoreFile_", this.cacheSize)) {
	    for (File file: replicatedFiles) {
		try (var scanner = new Scanner(file)) {
		    scanner.useDelimiter("\r\n");
		    while (scanner.hasNext()) {
			String key = scanner.next();
			String value = scanner.next();

			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte[] keyDigest = md.digest();

			if (serverKeyRange.withinKeyRange(keyDigest)) {
			    recoveryWriter.writeKV(key, value);		
			}
		    }
		}
	    }
	}

    };


    public String searchForKeyInFiles(String key, String prefix) throws Exception {
	
	File[] files = this.getFilesInReverseChronologicalOrder(prefix);

        for (File file: files) {
	    try (var scanner = new Scanner(file)) {
		scanner.useDelimiter("\r\n");
		while (scanner.hasNext()) {
		    String testKey = scanner.next();
		    String testValue = scanner.next();
		    if (key.equals(testKey)) {
			return testValue;
		    }
		}
	    }
        }

	return "null";

    }

    public void writeFileFromFileContents(String prefix, byte[][] fileContents) {

	File newFile = new File(this.dataDirectory, prefix + Instant.now().toString() + ".txt"); 
	try (FileOutputStream fileOutput = new FileOutputStream(newFile, true)) {
	    for (byte[] b: fileContents) {
		fileOutput.write(b); 
	    } 
	} catch (Exception e) {
	    logger.error("Failed to create new " + prefix + ": " + e.getMessage()); 
	}
    }

    public byte[][] fileTofileContentsMatrix(File file) {
	var fileContents = new ArrayList<byte[]>();
	try (FileInputStream fileInput = new FileInputStream(file)) {

	    var numMaxByteArrays = file.length() / 10000;

	    for (int i = 0; i < numMaxByteArrays; i++) {
		var b = fileInput.readNBytes(10000);
		fileContents.add(b);
	    }

	    var leftoverByteArraySize = (int) file.length() % 10000;

	    if (leftoverByteArraySize != 0) {
		var b = fileInput.readNBytes(leftoverByteArraySize);
		fileContents.add(b);
	    }

	} catch (Exception e) {
	    logger.error("Failure converting file to byte matrix:" + e.getMessage());	
	}

	byte[][] fileContentsByteMatrix = new byte[fileContents.size()][];
	for (int i = 0; i < fileContentsByteMatrix.length; i++) {
	    fileContentsByteMatrix[i] = fileContents.get(i);
	}

	return fileContentsByteMatrix;
	
    }

    public File[] getFilesInReverseChronologicalOrder(String prefix) {
	
	File[] files = filterFilesByPrefix(prefix);

        Arrays.sort(files, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return Long.valueOf(f2.lastModified()).compareTo(f1.lastModified());
            }
        });

	return files;


    }

    public File[] filterFilesByPrefix(String prefix) {

        File[] filteredFileList = this.dataDirectory.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(prefix);
            }
        });
	
	return filteredFileList;

    }

}

