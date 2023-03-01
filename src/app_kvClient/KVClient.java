package app_kvClient;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;
import java.security.*;

import client.ProtocolMessage;
import client.KVCommInterface;
import client.KVStore;
import shared.messages.KVMessage;
import shared.KeyRange;
import shared.messages.KVMessage.StatusType;

public class KVClient implements IKVClient {

    public static Logger logger = Logger.getRootLogger();
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
	} else if (value != null && value.getBytes().length > 120 * 1024) {
	    throw new IllegalArgumentException("Value length must be less than 120 kilobytes.");
	} else {
	    
	    KVMessage put_response = this.store.put(key,value);
	    
	    if(put_response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE){
	    	
		String ask = new String("keyrange\r\n");
	        
		this.store.askMetadata(ask);
		
		ProtocolMessage metadata = this.store.receiveMessage();
	
		List<String> servers = Arrays.asList(metadata.getKey().split(";"));
		
		for(var server: servers){
		    List<String> elems = Arrays.asList(server.split(","));
		    
		    int port = Integer.parseInt(elems.get(3));
		    byte[] rangeFrom = elems.get(0).getBytes("UTF-8");
		    byte[] rangeTo = elems.get(1).getBytes("UTF-8");

		    KeyRange test = new KeyRange(port,elems.get(2),rangeFrom,rangeTo);
		    System.out.println(String.format("elem[0]: %s, elem[1]: %s, elem[2]: %s, elem[3]: %s", elems.get(0),elems.get(1),elems.get(2),elems.get(3)));
		    if(test.withinKeyRange(this.hashKey(key))){
		    	this.disconnect();
			this.newConnection(elems.get(2),port);
			
		    }
			

		}
	

	    }
	    return this.store.put(key, value);
	}

    }
	
    private byte[] hashKey(String key) {
	try {
	    MessageDigest md = MessageDigest.getInstance("MD5");
	    md.update(key.getBytes());
	    return md.digest();
	} catch (Exception e) {
	    throw new RuntimeException("Error: Impossible NoSuchAlgorithmError!");
	}
    }

    public KVMessage get(String key) throws Exception {

	if (key.getBytes().length > 20) {
	    throw new IllegalArgumentException("Key length must be less than 20 bytes.");
	} else {
	   
		KVMessage get_response = this.store.get(key);

		if(get_response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE){
	    	
			String ask = new String("keyrange\r\n");
	        
			this.store.askMetadata(ask);
		
			ProtocolMessage metadata = this.store.receiveMessage();
	
			List<String> servers = Arrays.asList(metadata.getKey().split(";"));
		
			for(var server: servers){
		    		List<String> elems = Arrays.asList(server.split(","));
		    
		    		int port = Integer.parseInt(elems.get(3));
		    		byte[] rangeFrom = elems.get(0).getBytes("UTF-8");
		    		byte[] rangeTo = elems.get(1).getBytes("UTF-8");

		    		KeyRange test = new KeyRange(port,elems.get(2),rangeFrom,rangeTo);
		    		System.out.println(String.format("elem[0]: %s, elem[1]: %s, elem[2]: %s, elem[3]: %s", elems.get(0),elems.get(1),elems.get(2),elems.get(3)));
		    		if(test.withinKeyRange(this.hashKey(key))){
		    			this.disconnect();
					this.newConnection(elems.get(2),port);	
		    		}
			}	
		}
 
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
	    level.equals(Level.OFF)) {
	    this.logger.setLevel(level);
	} else {
	    throw new IllegalArgumentException(
		"Logger level must be one of ALL|INFO|WARN|DEBUG|ERROR|FATAL|OFF.");
	}

    }

    public String getLogLevel() {
	return logger.getLevel().toString();
    }

    public void help() {

    }

    public void quit() {

    }

}
