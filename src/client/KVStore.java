package client;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;

import org.apache.log4j.Logger;

import shared.*;
import shared.messages.KVMessage;

public class KVStore implements KVCommInterface {

	private static Logger logger = Logger.getRootLogger();

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private String currentAddress;
	private int currentPort;

	private TreeMap<byte[], KeyRange> metadata;
	private HashMap<KeyRange, Socket> connections;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	*/
	public KVStore(String address, int port) {
		this.currentAddress = address;	
		this.currentPort = port;
		this.metadata = new TreeMap<byte[], KeyRange>(new ByteArrayComparator());
		this.connections = new HashMap<KeyRange, Socket>();
	}

	@Override
	public void connect() throws Exception {
		Socket socket = new Socket(this.currentAddress, this.currentPort);

		ProtocolMessage keyRangeMessage = this.keyrange(socket);
		this.metadata = this.parseKeyRangeMessage(keyRangeMessage);

		KeyRange currentServerKeyRange = this.metadata.get(this.hashIP(this.currentAddress, this.currentPort));
		this.connections.put(currentServerKeyRange, socket);

	}

	@Override
	public void disconnect() {

		for (var entry: this.connections.entrySet()) {
			var socket = entry.getValue();
			try {
				socket.close();
			} catch (Exception e) {
				logger.error("Failed to gracefully close connection: " + e.getMessage());
			}
		}

	}

	@Override
	public KVMessage put(String key, String value) throws Exception {

		try {
			Socket targetSocket = this.identifySocketByKey(key);

			OutputStream output = targetSocket.getOutputStream();
			InputStream input = targetSocket.getInputStream();

			String p = new String("put " + key + " " + value + "\r\n"); 
			byte[] b = p.getBytes("UTF-8");

			output.write(b);
			output.flush();	

			logger.info("Sent protocol message: Put request with key = " + key + ", value = " + value); 

			ProtocolMessage putReply = this.receiveMessage(input);

			logger.info(String.format("Received protocol message: status = %s, key = %s, value = %s", putReply.getStatus(), putReply.getKey(), putReply.getValue())); 

			if (putReply.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				this.metadata = this.parseKeyRangeMessage(putReply);
				return this.put(key, value);
			}

			return putReply;

		} catch (Exception e) {
			ProtocolMessage keyRangeMessage = this.keyrange();
			this.metadata = this.parseKeyRangeMessage(keyRangeMessage);
			return this.put(key, value);
		}
	}

	@Override
	public KVMessage get(String key) throws Exception {

		try {

			Socket targetSocket = this.identifySocketByKey(key);

			OutputStream output = targetSocket.getOutputStream();
			InputStream input = targetSocket.getInputStream();

			String p = new String("get " + key + "\r\n"); 
			byte[] b = p.getBytes("UTF-8");

			output.write(b);
			output.flush();	

			logger.info("Sent protocol message: GET request with key = " + key + ", value = null"); 

			ProtocolMessage getReply = this.receiveMessage(input);

			logger.info(String.format("Received protocol message: status = %s, key = %s, value = %s", getReply.getStatus(), getReply.getKey(), getReply.getValue())); 

			if (getReply.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				this.metadata = this.parseKeyRangeMessage(getReply);
				return this.get(key);
			}

			return getReply;
		} catch (Exception e) {
			ProtocolMessage keyRangeMessage = this.keyrange();
			this.metadata = this.parseKeyRangeMessage(keyRangeMessage);
			return this.get(key);
		}
	}

	public ProtocolMessage keyrange(Socket socket) throws Exception {

		OutputStream output = socket.getOutputStream();
		InputStream input = socket.getInputStream();

		String keyrangeRequest = "keyrange\r\n";
		output.write(keyrangeRequest.getBytes());
		output.flush();

		ProtocolMessage keyrangeReply = this.receiveMessage(input);

		return keyrangeReply;

	}

	public ProtocolMessage keyrange() throws Exception {

		ProtocolMessage reply = null;

		var entryIterator = this.connections.entrySet().iterator();
		while (entryIterator.hasNext()) {
			
			var entry = entryIterator.next();
			var keyRange = entry.getKey();
			var socket = entry.getValue();

			try {
				reply = this.keyrange(socket);
			} catch (Exception e) {
				logger.debug(String.format("Failed to connect to node <%s:%d>", socket.getInetAddress().getHostName(), socket.getPort()));
				this.connections.remove(keyRange);
				return this.keyrange();
			}

		}

		if (reply == null) {
			throw new Exception("No available connections");
		}

		return reply;

	}

	public void connectToSocketByKeyRange(KeyRange keyRange) throws Exception {
		Socket socket = new Socket(keyRange.getAddress(), keyRange.getPort());
		this.connections.put(keyRange, socket);
	}

	public ProtocolMessage receiveMessage(InputStream input) throws Exception {

		int byteCount = 0;
		int index = 0;
		byte[] msgBuf = new byte[BUFFER_SIZE];

		byte prev_value = 0;
		byte cur_value = 0;

		while ((cur_value = (byte) input.read()) != -1) {

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

	private Socket identifySocketByKey(String key) throws Exception {

		byte[] hashedKey = this.hashKey(key); 

		var targetServerKeyRangeEntry = this.metadata.ceilingEntry(hashedKey);

		if (targetServerKeyRangeEntry == null) {
			targetServerKeyRangeEntry = this.metadata.firstEntry();	
		}

		var targetServerKeyRange = targetServerKeyRangeEntry.getValue();

		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < targetServerKeyRange.getRangeFrom().length; i++) {
			sb.append(String.format("%02x", targetServerKeyRange.getRangeFrom()[i]));
		}

		logger.debug(String.format("key %s hashes to %s at <%s:%d>", key, sb.toString(), targetServerKeyRange.getAddress(), targetServerKeyRange.getPort()));

		if (!this.connections.containsKey(targetServerKeyRange)) {
			this.connectToSocketByKeyRange(targetServerKeyRange);
		}

		return this.connections.get(targetServerKeyRange);
	}

	private TreeMap<byte[], KeyRange> parseKeyRangeMessage(ProtocolMessage message) throws Exception {

		var metadata = new TreeMap<byte[], KeyRange>(new ByteArrayComparator());

		String keyRangeMessage = message.getKey();

		List<String> servers = Arrays.asList(keyRangeMessage.split(";"));

	for(var server: servers) {

			List<String> elems = Arrays.asList(server.split(","));

			byte[] rangeFrom = this.parseHexString(elems.get(0));
			byte[] rangeTo = this.parseHexString(elems.get(1));

			List<String> AddressAndPort = Arrays.asList(elems.get(2).split(":"));

			String address = AddressAndPort.get(0);
			int port = Integer.parseInt(AddressAndPort.get(1));

			KeyRange nodeRange = new KeyRange(port, address, rangeFrom, rangeTo);

			metadata.put(rangeFrom, nodeRange);

		}

		return metadata;

	}

	private byte[] parseHexString(String hexString) {

		byte[] byteArray = new byte[16];

		for (int i = 0; i < 16; i++) {

			int firstDigit = hexString.charAt(i*2);
			int secondDigit = hexString.charAt((i*2) + 1);

		if (firstDigit >= '0' && firstDigit <= '9') {
				firstDigit -= '0';
			} else if (firstDigit >= 'a' && firstDigit <= 'f') {
				firstDigit -= ('a' - 10);
			}

			if (secondDigit >= '0' && secondDigit <= '9') {
				secondDigit -= '0';
			} else if (secondDigit >= 'a' && secondDigit <= 'f') {
				secondDigit -= ('a' - 10);
			}

			byteArray[i] = (byte) ((firstDigit << 4) | (secondDigit));

		}

		return byteArray;

	}

	private byte[] hashIP(String address, int port) throws Exception {

		try {
			String valueToHash = address + ":" + Integer.toString(port);	
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(valueToHash.getBytes());
			return md.digest();
		} catch (Exception e) {
			throw new RuntimeException("Error: Impossible NoSuchAlgorithmError!");
		}

	}

	private byte[] hashKey(String key) throws Exception {

		try {
			String valueToHash = key;
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(valueToHash.getBytes());
			return md.digest();
		} catch (Exception e) {
			throw new RuntimeException("Error: Impossible NoSuchAlgorithmError!");
		}

	}
}
