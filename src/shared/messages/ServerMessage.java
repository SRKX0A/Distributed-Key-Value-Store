package shared.messages;

import java.io.*;
import java.util.*;

import client.ClientSubscriptionInfo;

public class ServerMessage implements Serializable {

    private static final long serialVersionUID = 0x419;

    public enum StatusType {
	SEND_KV,
	SEND_REPLICA_KV_1,
	SEND_REPLICA_KV_2,
	SERVER_INIT_FIN,
	REPLICATE_KV_1,
	REPLICATE_KV_2,
	REPLICATE_KV_1_FIN,
	REPLICATE_KV_2_FIN,
	SEND_SUBSCRIPTIONS,
	REPLICATE_SUBSCRIPTIONS, 
    }

    public ServerMessage(StatusType status, byte[][] fileContents, TreeMap<String, List<ClientSubscriptionInfo>> subscriptions) {
	this.status = status;
	if (fileContents != null) {
	    this.fileContents = Arrays.stream(fileContents).map(byte[]::clone).toArray(byte[][]::new);
	}
	this.subscriptions = subscriptions;
    }

    private StatusType status;
    private byte[][] fileContents;
    private TreeMap<String, List<ClientSubscriptionInfo>> subscriptions;

    public StatusType getStatus() {
	return this.status;
    }

    public byte[][] getFileContents() {
	return this.fileContents;
    }

    public TreeMap<String, List<ClientSubscriptionInfo>> getSubscriptions() {
	return this.subscriptions;
    }

}
