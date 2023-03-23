package app_kvServer.util;

import java.io.*;
import java.time.*;

import org.apache.log4j.Logger;

class LimitedKVBufferedWriter implements AutoCloseable {

    private static Logger logger = Logger.getRootLogger();

    private final File fileDirectory;
    private final String prefix;
    private final int limit;

    private File currentFile;
    private BufferedWriter currentWriter;
    private int writeCount;
    
    public LimitedKVBufferedWriter(File fileDirectory, String prefix, int limit) throws Exception {
	this.fileDirectory = fileDirectory;
	this.prefix = prefix;
	this.limit = limit;

	this.currentFile = new File(this.fileDirectory, this.prefix + Instant.now().toString() + ".txt"); 
	this.currentFile.createNewFile();
	this.currentWriter = new BufferedWriter(new FileWriter(this.currentFile, true));
	this.writeCount = 0;
    }

    public void writeKV(String key, String value) throws Exception {

	this.currentWriter.write(String.format("%s\r\n%s\r\n", key, value));
	this.writeCount++;

	if (this.writeCount >= this.limit) {
	    this.currentWriter.close();
	    this.currentFile = new File(this.fileDirectory, prefix + Instant.now().toString() + ".txt"); 
	    this.currentFile.createNewFile();
	    this.currentWriter = new BufferedWriter(new FileWriter(this.currentFile, true));
	    this.writeCount = 0;
	}
    }

    public void close() {
	try {
	    this.currentWriter.close();

	    if (this.currentFile.length() == 0) {
		this.currentFile.delete();
	    }

	} catch (Exception e) {
	    logger.error("Failed to close LimitedKVBufferedWriter: " + e.getMessage());
	}
    }

}
