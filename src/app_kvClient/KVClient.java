package app_kvClient;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVCommInterface;
import client.KVStore;
import shared.messages.KVMessage;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
    private KVStore store;

    @Override
    public KVCommInterface getStore() {
	return this.store;
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception {
	this.store = new KVStore(hostname, port);
	this.store.connect();
    }

    public void disconnect() {
	this.store.disconnect();
    }

    public KVMessage put(String key, String value) throws Exception {

	if (key.getBytes().length > 20) {
	    throw new IllegalArgumentException("Key length must be less than 20 bytes.");
	} else if (value != null && value.getBytes().length > 120*1024) {
	    throw new IllegalArgumentException("Value length must be less than 120 kilobytes.");
	} else {
	    return this.store.put(key, value);
	}

    }

    public KVMessage get(String key) throws Exception {

	if (key.getBytes().length > 20) {
	    throw new IllegalArgumentException("Key length must be less than 20 bytes.");
	} else {
	    return this.store.get(key);
	}

    }

    public void logLevel(Level level) throws Exception {

	if (level.equals(Level.ALL) ||
	    level.equals(Level.INFO) ||
	    level.equals(Level.WARN) ||
	    level.equals(Level.DEBUG) ||
	    level.equals(Level.ERROR) ||
	    level.equals(Level.FATAL) ||
	    level.equals(Level.OFF)
	    ) {
	    this.logger.setLevel(level);
	} else {
	    throw new IllegalArgumentException(
	    "Logger level must be one of ALL|INFO|WARN|DEBUG|ERROR|FATAL|OFF.");
	}

    }

    public void help() {

    }

    public void quit() {

    }

}
