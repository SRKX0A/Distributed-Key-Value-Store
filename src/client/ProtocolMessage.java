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
	}

	int indexOfFirstSpace = msgString.indexOf(" ");

	if (indexOfFirstSpace == -1) {
	    throw new IllegalArgumentException("Error: Request type must be either PUT or GET");
	}

	String status = msgString.substring(0, indexOfFirstSpace);

	switch (status.toLowerCase()) {
	    case "put":
		protocolStatus = KVMessage.StatusType.PUT;
		break;
	    case "get":
		protocolStatus = KVMessage.StatusType.GET;
		break;
	    case "server_init":
		protocolStatus = KVMessage.StatusType.SERVER_INIT;
		break;
	    case "send_kv":
		protocolStatus = KVMessage.StatusType.SEND_KV;
		break;
	    case "send_kv_fin":
		protocolStatus = KVMessage.StatusType.SEND_KV_FIN;
		break;
	    case "send_replica_kv_1":
		protocolStatus = KVMessage.StatusType.SEND_REPLICA_KV_1;
		break;
	    case "send_replica_kv_1_fin":
		protocolStatus = KVMessage.StatusType.SEND_REPLICA_KV_1_FIN;
		break;
	    case "send_replica_kv_2":
		protocolStatus = KVMessage.StatusType.SEND_REPLICA_KV_2;
		break;
	    case "send_replica_kv_2_fin":
		protocolStatus = KVMessage.StatusType.SEND_REPLICA_KV_2_FIN;
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
	    case "replicate_kv_1":
		protocolStatus = KVMessage.StatusType.REPLICATE_KV_1;
		break;
	    case "replicate_kv_2":
		protocolStatus = KVMessage.StatusType.REPLICATE_KV_2;
		break;
	    case "replicate_kv_fin":
		protocolStatus = KVMessage.StatusType.REPLICATE_KV_FIN;
		break;
	    default:
		throw new IllegalArgumentException("Error: Request type must be either PUT or GET");
	}

	if (protocolStatus == KVMessage.StatusType.PUT
	    || protocolStatus == KVMessage.StatusType.SEND_KV
	    || protocolStatus == KVMessage.StatusType.SEND_REPLICA_KV_1
	    || protocolStatus == KVMessage.StatusType.SEND_REPLICA_KV_2
	    || protocolStatus == KVMessage.StatusType.REPLICATE_KV_1
	    || protocolStatus == KVMessage.StatusType.REPLICATE_KV_2) {

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
	} else if (status.toLowerCase().equals("server_not_responsible")) {
	    protocolStatus = StatusType.SERVER_NOT_RESPONSIBLE;
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

	} else if (protocolStatus == StatusType.KEYRANGE_SUCCESS) {

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

	if (this.status == StatusType.KEYRANGE_SUCCESS || this.status == StatusType.REPLICATE_KV_HANDSHAKE) {
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
