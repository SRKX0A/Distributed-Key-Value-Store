package client;

import java.io.*;
import java.net.Socket;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;

public class KVStore implements KVCommInterface {

	private static Logger logger = Logger.getRootLogger();

	final String address;
	final int port;

	Socket socket;

	InputStream input;
	OutputStream output;

	ObjectInputStream ois;
	ObjectOutputStream oos;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
	    this.address = address;	
	    this.port = port;
	}

	@Override
	public void connect() throws Exception {
	    this.socket = new Socket(this.address, this.port);
	    this.input = this.socket.getInputStream();
	    this.output = this.socket.getOutputStream();
	    this.ois = new ObjectInputStream(this.input);	
	    this.oos = new ObjectOutputStream(this.input);	
	}

	@Override
	public void disconnect() {
	    try {
		this.oos.close(); 
		this.ois.close(); 
		this.socket.close();
	    } catch (Exception e) {
		this.logger.error(e.toString());
	    }
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
	    ProtocolMessage put_request = new ProtocolMessage(KVMessage.StatusType.PUT, key, value); 

	    this.oos.writeObject(put_request);
	    this.oos.write('\r');
	    this.oos.write('\n');
	    this.oos.flush();

	    this.logger.info("Sent protocol message: Put request with key = " + put_request.getKey() + ", value = " + put_request.getValue()); 

	    ProtocolMessage put_reply = (ProtocolMessage) ois.readObject();
	    this.ois.skipBytes(2);

	    this.logger.info("Received protocol message: status = " + put_reply.getStatus()); 

	    return put_reply;
	}

	@Override
	public KVMessage get(String key) throws Exception {
	    ProtocolMessage get_request = new ProtocolMessage(KVMessage.StatusType.GET, key, null);

	    this.oos.writeObject(get_request);
	    this.oos.write('\r');
	    this.oos.write('\n');
	    this.oos.flush();

	    this.logger.info("Sent protocol message: GET request with key = " + get_request.getKey() + ", value = " + get_request.getValue()); 

	    ProtocolMessage get_reply = (ProtocolMessage) this.ois.readObject();
	    this.ois.skipBytes(2);

	    this.logger.info("Received protocol message: status = " + get_reply.getStatus() + ", value = " + get_reply.getValue()); 

	    return get_reply;
	}
}
