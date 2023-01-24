package client;

import java.io.Serializable;

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
