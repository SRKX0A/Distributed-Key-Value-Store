package shared.messages;

import java.io.*;

public class SubscriptionMessage implements Serializable {

    public enum StatusType {
        KV_NOTIFICATION,
    }

    private static final long serialVersionUID = 0x419;
    private StatusType status;
    private String key;
    private String oldValue;
    private String newValue;

    public SubscriptionMessage(StatusType status, String key, String oldValue, String newValue) {
        this.status = status;
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

	public StatusType getStatus() {
		return status;
	}

	public String getKey() {
		return key;
	}

	public String getOldValue() {
		return oldValue;
	}

	public String getNewValue() {
		return newValue;
	}

}
