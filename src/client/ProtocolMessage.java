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

    public static ProtocolMessage fromBytesAtServer(byte[] buf) throws Exception {

	StatusType protocolStatus = null;
	String protocolKey = null;
	String protocolValue = null;

	String msgString = new String(buf, StandardCharsets.UTF_8);

	if (msgString.equals("keyrange\r\n")) {
	    return new ProtocolMessage(StatusType.KEYRANGE, null, null);
	} else if (msgString.equals("keyrange_read\r\n")) {
	    return new ProtocolMessage(StatusType.KEYRANGE_READ, null, null);
	}
	
	int indexOfFirstSpace = msgString.indexOf(" ");

	if (indexOfFirstSpace == -1) {
	    throw new IllegalArgumentException("Error: Invalid request type");
	}

	String status = msgString.substring(0, indexOfFirstSpace);

	switch (status.toLowerCase()) {
	    case "put":
		protocolStatus = KVMessage.StatusType.PUT;
		break;
	    case "get":
		protocolStatus = KVMessage.StatusType.GET;
		break;
	    case "subscribe":
		protocolStatus = KVMessage.StatusType.SUBSCRIBE;
		break;
	    case "unsubscribe":
		protocolStatus = KVMessage.StatusType.UNSUBSCRIBE;
		break;
	    case "server_init":
		protocolStatus = KVMessage.StatusType.SERVER_INIT;
		break;
	    case "replicate_kv_handshake":
		protocolStatus = KVMessage.StatusType.REPLICATE_KV_HANDSHAKE;
		break;
	    case "replicate_kv_handshake_ack":
		protocolStatus = KVMessage.StatusType.REPLICATE_KV_HANDSHAKE_ACK;
		break;
	    case "replicate_kv_handshake_nack":
		protocolStatus = KVMessage.StatusType.REPLICATE_KV_HANDSHAKE_NACK;
		break;
	    default:
		throw new IllegalArgumentException("Error: Request type must be either PUT or GET");
	}

	if (protocolStatus == KVMessage.StatusType.PUT) {

	    int indexOfSecondSpace = msgString.indexOf(" ", indexOfFirstSpace + 1);

	    if (indexOfSecondSpace == -1) {
		throw new IllegalArgumentException("Error: PUT request must have an associated value");
	    }

	    String key = msgString.substring(indexOfFirstSpace + 1, indexOfSecondSpace);

	    if (key.getBytes().length > 20) {
		throw new IllegalArgumentException("Error: Key must be less than or equal to 20 bytes");
	    }

	    protocolKey = key;

	    String value = msgString.substring(indexOfSecondSpace + 1);

	    if (value.getBytes().length > 120 * 1024) {
		throw new IllegalArgumentException("Error: Value must be less than or equal to 120 kilobytes");
	    } else if (!value.endsWith("\r\n")) {
		throw new IllegalArgumentException("Error: Malformed message");
	    } else {
		protocolValue = value.substring(0, value.length() - 2);
	    }

	} else if (protocolStatus == KVMessage.StatusType.GET) {
	   
	    String key = msgString.substring(indexOfFirstSpace + 1);

	    if (key.getBytes().length > 20) {
		throw new IllegalArgumentException("Error: Key must be less than or equal to 20 bytes");
	    } else if (!key.endsWith("\r\n")) {
		throw new IllegalArgumentException("Error: Malformed message");
	    }

	    protocolKey = key.substring(0, key.length() - 2);
	    protocolValue = "null";

	} else if (protocolStatus == KVMessage.StatusType.SUBSCRIBE || protocolStatus == KVMessage.StatusType.UNSUBSCRIBE) {

	    int indexOfSecondSpace = msgString.indexOf(" ", indexOfFirstSpace + 1);

	    if (indexOfSecondSpace == -1) {
		throw new IllegalArgumentException("Error: " + protocolStatus.toString() + " request must have an associated address and port");
	    }
	    
	    String key = msgString.substring(indexOfFirstSpace + 1, indexOfSecondSpace);

	    if (key.getBytes().length > 20) {
		throw new IllegalArgumentException("Error: Key must be less than or equal to 20 bytes");
	    }

	    protocolKey = key;

	    String value = msgString.substring(indexOfSecondSpace + 1);

	    if (value.getBytes().length > 120 * 1024) {
		throw new IllegalArgumentException("Error: address and port must be less than or equal to 120 kilobytes");
	    } else if (!value.endsWith("\r\n")) {
		throw new IllegalArgumentException("Error: Malformed message");
	    } else {
		protocolValue = value.substring(0, value.length() - 2);
	    }
	
	} else if (protocolStatus == KVMessage.StatusType.REPLICATE_KV_HANDSHAKE) {

	    String key = msgString.substring(indexOfFirstSpace + 1);

	    if (!key.endsWith("\r\n")) {
		throw new IllegalArgumentException("Error: Malformed message");
	    }

	    protocolKey = key.substring(0, key.length() - 2);
	    protocolValue = "null";
	
	}

	return new ProtocolMessage(protocolStatus, protocolKey, protocolValue);

    }

    public static ProtocolMessage fromBytesAtClient(byte[] buf) throws Exception {

	StatusType protocolStatus = null;
	String protocolKey = null;
	String protocolValue = null;

	String msgString = new String(buf, StandardCharsets.UTF_8);

	if (msgString.equals("SERVER_NOT_RESPONSIBLE\r\n")) {
	    return new ProtocolMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
	}

	int indexOfFirstSpace = msgString.indexOf(" ");
	String status = msgString.substring(0, indexOfFirstSpace);

	if (status.toLowerCase().equals("put_success")) {
	    protocolStatus = KVMessage.StatusType.PUT_SUCCESS;
	} else if (status.toLowerCase().equals("put_update")) {
	    protocolStatus = StatusType.PUT_UPDATE;
	} else if (status.toLowerCase().equals("put_error")) {
	    protocolStatus = StatusType.PUT_ERROR;
	} else if (status.toLowerCase().equals("get_success")) {
	    protocolStatus = StatusType.GET_SUCCESS;
	} else if (status.toLowerCase().equals("get_error")) {
	    protocolStatus = StatusType.GET_ERROR;
	} else if (status.toLowerCase().equals("failed")) {
	    protocolStatus = StatusType.FAILED;
	} else if (status.toLowerCase().equals("keyrange_success")) {
	    protocolStatus = StatusType.KEYRANGE_SUCCESS;
	} else if (status.toLowerCase().equals("keyrange_read_success")) {
	    protocolStatus = StatusType.KEYRANGE_READ_SUCCESS;
	} else if (status.toLowerCase().equals("server_not_responsible")) {
	    protocolStatus = StatusType.SERVER_NOT_RESPONSIBLE;
	} else if (status.toLowerCase().equals("subscribe_success")) {
	    protocolStatus = StatusType.SUBSCRIBE_SUCCESS;
	} else if (status.toLowerCase().equals("subscribe_error")) {
	    protocolStatus = StatusType.SUBSCRIBE_ERROR;
    	} else if (status.toLowerCase().equals("unsubscribe_success")) {
	    protocolStatus = StatusType.UNSUBSCRIBE_SUCCESS;
    	} else if (status.toLowerCase().equals("unsubscribe_error")) {
	    protocolStatus = StatusType.UNSUBSCRIBE_ERROR;
	} else {
	    throw new IllegalArgumentException("Error: Malformed StatusType response from server");
	}

	if (protocolStatus == StatusType.PUT_SUCCESS || protocolStatus == StatusType.PUT_UPDATE || protocolStatus == StatusType.PUT_ERROR || protocolStatus == StatusType.GET_SUCCESS) {

	    int indexOfSecondSpace = msgString.indexOf(" ", indexOfFirstSpace + 1);
	    String key = msgString.substring(indexOfFirstSpace + 1, indexOfSecondSpace);

	    if (key.getBytes().length > 20) {
		throw new IllegalArgumentException("Error: Key must be less than or equal to 20 bytes");
	    }

	    protocolKey = key;

	    String value = msgString.substring(indexOfSecondSpace + 1);

	    if (value.getBytes().length > 120 * 1024) {
		throw new IllegalArgumentException("Error: Value must be less than or equal to 120 kilobytes");
	    } else if (!value.endsWith("\r\n")) {
		throw new IllegalArgumentException("Error: Malformed message from server");
	    } else {
		protocolValue = value.substring(0, value.length() - 2);
	    }
	} else if (protocolStatus == StatusType.GET_ERROR) {
	   
	    String key = msgString.substring(indexOfFirstSpace + 1);

	    if (key.getBytes().length > 20) {
		throw new IllegalArgumentException("Error: Key must be less than or equal to 20 bytes");
	    } else if (!key.endsWith("\r\n")) {
		throw new IllegalArgumentException("Error: Malformed message from server");
	    }

	    protocolKey = key.substring(0, key.length() - 2);
	    protocolValue = "null";

	} else if (protocolStatus == StatusType.KEYRANGE_SUCCESS || protocolStatus == StatusType.KEYRANGE_READ_SUCCESS) {

	    String key = msgString.substring(indexOfFirstSpace + 1);

	    if (!key.endsWith("\r\n")) {
		throw new IllegalArgumentException("Error: Malformed message from server");
	    }

	    protocolKey = key.substring(0, key.length() - 2);
	    protocolValue = "null";

	} else if (protocolStatus == StatusType.SERVER_NOT_RESPONSIBLE) {

	    if (!msgString.endsWith("\r\n")) {
		throw new IllegalArgumentException("Error: Malformed message from server");
	    }

	    protocolKey = "null";
	    protocolValue = "null";

	} else if (protocolStatus == StatusType.SUBSCRIBE_SUCCESS || protocolStatus == StatusType.UNSUBSCRIBE_SUCCESS || protocolStatus == StatusType.SUBSCRIBE_ERROR || protocolStatus == StatusType.UNSUBSCRIBE_ERROR) {

	   if (!msgString.endsWith("\r\n")){
	   	throw new IllegalArgumentException("Error: Malformed message from server");
	   }

	   protocolKey = "null";
	   protocolValue = "null";

	} else {

	    String key = msgString.substring(indexOfFirstSpace + 1);

	    if (!key.endsWith("\r\n")) {
		throw new IllegalArgumentException("Error: Malformed message from server");
	    }

	    protocolKey = key.substring(0, key.length() - 2);
	    protocolValue = "null";

	}

	return new ProtocolMessage(protocolStatus, protocolKey, protocolValue);

    }

    public byte[] getBytes() throws Exception {

	String msgString = null;

	if (this.status == StatusType.KEYRANGE_SUCCESS || this.status == StatusType.KEYRANGE_READ_SUCCESS || this.status == StatusType.REPLICATE_KV_HANDSHAKE) {
	    msgString = this.status.toString() + " " + this.key + "\r\n";     
	} else if (this.status == StatusType.SERVER_NOT_RESPONSIBLE) {
	    msgString = this.status.toString() + "\r\n";
	} else {
	    msgString = this.status.toString() + " " + this.key + " " + this.value + "\r\n";
	}

	return msgString.getBytes("UTF-8");
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
