package client;

public class ClientSubscriptionInfo {
	
    private String address;
    private int port;

    public Subscribe(String address, int port) {
	this.address = address;
	this.port = port;
    }

    public boolean equals(Object o) {
	var other = (ClientSubscriptionInfo) o;
	return other.getAddress().equals(this.address) && other.getPort() == this.port;
    }

    public String toString() {
	return "<" + this.address + ":" + Integer.toString(this.port) + ">";
    }

    public String getAddress() {
	return this.address;
    }

    public int getPort() {
	return this.port;
    }

}
