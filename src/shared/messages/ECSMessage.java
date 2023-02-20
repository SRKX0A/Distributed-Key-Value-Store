package shared.messages;

import java.io.*;
import java.util.*;

import shared.KeyRange;

public class ECSMessage implements Serializable {

    public enum StatusType {
	REBALANCE_INIT_REQ,
	REBALANCE_TERM_REQ,
	REBALANCE,
	REBALANCE_ACK,
	REBALANCE_FIN,
	METADATA,
	METADATA_ACK,
    }

    public ECSMessage(StatusType status, ArrayList<KeyRange> metadata, int metadataIndex) {
	this.status = status;
	this.metadata = metadata;
	this.metadataIndex = metadataIndex;
    }

    private StatusType status;
    private ArrayList<KeyRange> metadata;
    private int metadataIndex;

    public StatusType getStatus() {
	return this.status;
    }
    public ArrayList<KeyRange> getMetadata() {
	return this.metadata;
    }
    public int getMetadataIndex() {
	return this.metadataIndex;
    }

}
