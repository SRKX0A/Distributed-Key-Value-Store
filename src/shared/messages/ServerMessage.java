package shared.messages;

import java.io.*;
import java.util.*;

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
    }

    public ServerMessage(StatusType status, byte[][] fileContents) {
	this.status = status;
	if (fileContents != null) {
	    this.fileContents = Arrays.stream(fileContents).map(byte[]::clone).toArray(byte[][]::new);
	}
    }

    private StatusType status;
    private byte[][] fileContents;

    public StatusType getStatus() {
	return this.status;
    }

    public byte[][] getFileContents() {
	return this.fileContents;
    }

}
