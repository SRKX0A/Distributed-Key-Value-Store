package app_kvServer;

import java.io.*;
import java.net.*;

import org.apache.log4j.Logger;

import client.ProtocolMessage;
import shared.messages.KVMessage;

public class Connection extends Thread {
    
    private static Logger logger = Logger.getRootLogger();
    
    private KVServer kvServer;

    private Socket socket;

    private InputStream input;
    private OutputStream output;

    private boolean isOpen;

    public Connection(Socket socket, KVServer kvServer) throws IOException {
	this.kvServer = kvServer;

	this.socket = socket;
	this.input = socket.getInputStream();
	this.output = socket.getOutputStream();

	this.isOpen = true;
    }

    public void run() {

	while (this.isOpen) {

	    try {

		ObjectInputStream ois = new ObjectInputStream(this.input);

		ProtocolMessage request = (ProtocolMessage) ois.readObject();
		ois.skipBytes(2);

		this.logger.info("Received protocol message: status = " + request.getStatus() + ", key = " + request.getKey() + ", value = " + request.getValue()); 
		// TODO: handle incoming pm

		//placeholder reply message

		ProtocolMessage reply = new ProtocolMessage(KVMessage.StatusType.PUT_SUCCESS, null, null);

		ObjectOutputStream oos = new ObjectOutputStream(this.output);

		oos.writeObject(reply);
		oos.write('\r');
		oos.write('\n');
		oos.flush();

		this.logger.info("Sent protocol message: Put reply with status = " + reply.getStatus());

	    } catch(Exception e) {
		logger.error(e.toString());
		return;
	    }

	}

    }
}
