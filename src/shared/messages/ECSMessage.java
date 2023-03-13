package shared.messages;

import java.io.*;
import java.util.*;

import shared.KeyRange;

public class ECSMessage implements Serializable {

    private static final long serialVersionUID = 0x419;

    public enum StatusType {
	INIT_REQ,
	TERM_REQ,
	REQ_FIN,
	METADATA_LOCK,
	METADATA_UPDATE,
	SHUTDOWN,
	INVALID_REQUEST_TYPE,
	INVALID_MESSAGE_FORMAT,
    }

    public ECSMessage(StatusType status, String address, int port, TreeMap<byte[], KeyRange> metadata, byte[] ringPosition) {
	this.status = status;
	this.address = address;
	this.port = port;
	this.metadata = metadata;
	this.ringPosition = ringPosition;
    }

    private StatusType status;
    private String address;
    private int port;
    private TreeMap<byte[], KeyRange> metadata;
    private byte[] ringPosition;

    public StatusType getStatus() {
	return this.status;
    }

    public String getAddress() {
	return this.address;
    }

    public int getPort() {
	return this.port;
    }

    public TreeMap<byte[], KeyRange> getMetadata() {
	return this.metadata;
    }
    public byte[] getRingPosition() {
	return this.ringPosition;
    }

}
