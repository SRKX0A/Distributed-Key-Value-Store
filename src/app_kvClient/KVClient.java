package app_kvClient;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import client.KVCommInterface;

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
	} else if (value.getBytes().length > 120*1024) {
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
	
	switch (level) {
	    case Level.ALL:
	    case Level.INFO:
	    case Level.WARN:
	    case Level.DEBUG:
	    case Level.ERROR:
	    case Level.FATAL:
	    case Level.OFF:
		this.logger.setLevel(level);
		break;
	    default:
		throw new IllegalArgumentException(
		"Logger level must be one of ALL|INFO|WARN|DEBUG|ERROR|FATAL|OFF.");
	}

    }

    public void help() {

    }

    public void quit() {

    }

}
