package client;

import java.io.*;
import java.net.Socket;
import java.util.*;

import org.apache.log4j.Logger;

import shared.KeyRange;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {

	private static Logger logger = Logger.getRootLogger();

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

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
	    try {
		this.output.close(); 
		this.input.close(); 
		this.socket.close();
	    } catch (Exception e) {
		this.logger.error(e.toString());
	    }
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
    
	    String p = new String("put " + key + " " + value + "\r\n"); 
	    byte[] b = p.getBytes("UTF-8");

	    this.output.write(b);
	    this.output.flush();	

	    this.logger.info("Sent protocol message: Put request with key = " + key + ", value = " + value); 

	    ProtocolMessage put_reply = this.receiveMessage();
	    
	    this.logger.info(String.format("Received protocol message: status = %s, key = %s, value = %s", put_reply.getStatus(), put_reply.getKey(), put_reply.getValue())); 
	   
	    return put_reply;
	   
	}

    @Override
    public KVMessage get(String key) throws Exception {

	String p = new String("get " + key + "\r\n"); 
	byte[] b = p.getBytes("UTF-8");

	this.output.write(b);
	this.output.flush();	

	this.logger.info("Sent protocol message: GET request with key = " + key + ", value = null"); 

	ProtocolMessage get_reply = this.receiveMessage();

	this.logger.info(String.format("Received protocol message: status = %s, key = %s, value = %s", get_reply.getStatus(), get_reply.getKey(), get_reply.getValue())); 

	return get_reply;
    }
    
    public void askMetadata(String key) throws Exception{
    
	byte[] askb = key.getBytes("UTF-8");
	this.output.write(askb);
	this.output.flush();
	this.logger.info(String.format("Asked for metadata for key: %s", key));
    } 

    public ProtocolMessage receiveMessage() throws Exception {

	int byteCount = 0;
	int index = 0;
	byte[] msgBuf = new byte[BUFFER_SIZE];

	byte prev_value = 0;
	byte cur_value = 0;

	while ((cur_value = (byte) this.input.read()) != -1) {

	    msgBuf[byteCount++] = cur_value;
	    index++;

	    if (byteCount > DROP_SIZE) {
		break;
	    }

	    if (prev_value == 13 && cur_value == 10) {
		break;
	    }

	    if (index == BUFFER_SIZE) {
		byte[] tmpBuf = new byte[BUFFER_SIZE + byteCount];
		System.arraycopy(msgBuf, 0, tmpBuf, 0, byteCount);
		msgBuf = tmpBuf;
		index = 0;
	    }

	    prev_value = cur_value;

	}

	if (cur_value == -1 && byteCount == 0) {
	    throw new EOFException("EOF reached");
	}

	byte[] tmpBuf = new byte[byteCount];
	System.arraycopy(msgBuf, 0, tmpBuf, 0, byteCount);
	msgBuf = tmpBuf;

	return ProtocolMessage.fromBytesAtClient(msgBuf);
    }
}
