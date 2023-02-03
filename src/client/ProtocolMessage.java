package client;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import shared.messages.KVMessage;

public class ProtocolMessage implements Serializable, KVMessage {

    private static final long serialVersionUID = 0x419;
   
    private StatusType status; 
    private String key;
    private String value;

    public ProtocolMessage(StatusType status, String key, String value) {
	this.status = status;
	this.key = key;
	this.value = value;
    }

    public ProtocolMessage(byte[] buf) throws Exception {

	String msgString = new String(buf, StandardCharsets.UTF_8);

	int indexOfFirstSpace = msgString.indexOf(" ");
	String status = msgString.substring(0, indexOfFirstSpace);

	if (status.toLowerCase() == "put") {
	    this.status = KVMessage.StatusType.PUT;
	} else if (status.toLowerCase() == "get") {
	    this.status = KVMessage.StatusType.GET;
	} else {
	    throw new IllegalArgumentException("Error: Request type must be either PUT or GET");
	}

	int indexOfSecondSpace = msgString.indexOf(" ", indexOfFirstSpace + 1);
	String key = msgString.substring(indexOfFirstSpace + 1, indexOfSecondSpace);

	if (key.getBytes().length > 20) {
	    throw new IllegalArgumentException("Error: Key must be less than or equal to 20 bytes");
	}

	this.key = key;

	String value = msgString.substring(indexOfSecondSpace + 1);

	if (value.getBytes().length > 120 * 1024) {
	    throw new IllegalArgumentException("Error: Value must be less than or equal to 120 kilobytes");
	} else if (!value.endsWith("\r\n")) {
	    throw new IllegalArgumentException("Error: Malformed message");
	} else {
	    this.value = value.substring(0, value.length() - 2);
	}

    }

    public byte[] getBytes() throws Exception {
	String msgString = this.status.toString() + " " + this.key + " " + this.value + "\r\n";
	byte[] msgBytes = msgString.getBytes("UTF-8");
	return msgBytes;
    }

    public String getKey() {
	return this.key;
    }

    public String getValue() {
	return this.value;
    }

    public StatusType getStatus() {
	return this.status;
    }

}
