package client;

import java.io.*;
import java.net.Socket;

import shared.messages.KVMessage;

public class KVStore implements KVCommInterface {

	final String address;
	final int port;

	Socket socket;

	InputStream input;
	OutputStream output;

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
	}

	@Override
	public void disconnect() {
	    this.socket.close();
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
	    ProtocolMessage put_request = new ProtocolMessage(KVMessage.StatusType.PUT, key, value); 

	    ByteArrayOutputStream bos = new ByteArrayOutputStream(this.socket.output);	
	    ObjectOutputStream oos = new ObjectOutputStream(bos);
	    oos.writeObject(put_request);
	    oos.write('\r');
	    oos.write('\n');
	    oos.flush();
	    oos.close();

	    ByteArrayInputStream bis = new ByteArrayInputStream(this.socket.input);
	    ObjectInputStream ois = new ObjectInputStream(bis);
	    ProtocolMessage put_reply = ois.readObject();
	    ois.skipBytes(2);
	    ois.close();

	    return put_reply;
	}

	@Override
	public KVMessage get(String key) throws Exception {
	    ProtocolMessage get_request = new ProtocolMessage(KVMessage.StatusType.GET, key, null);

	    ByteArrayOutputStream bos = new ByteArrayOutputStream(this.socket.output);	
	    ObjectOutputStream oos = new ObjectOutputStream(bos);
	    oos.writeObject(get_request);
	    oos.write('\r');
	    oos.write('\n');
	    oos.flush();
	    oos.close();

	    ByteArrayInputStream bis = new ByteArrayInputStream(this.socket.input);
	    ObjectInputStream ois = new ObjectInputStream(bis);
	    ProtocolMessage get_reply = ois.readObject();
	    ois.skipBytes(2);
	    ois.close();

	    return get_reply;
	}
}
