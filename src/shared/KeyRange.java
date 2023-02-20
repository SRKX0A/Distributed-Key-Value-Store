package shared;

import java.math.BigInteger;

public class KeyRange {

    String address;
    int port;
    byte[] rangeFrom;
    byte[] rangeTo;

    public KeyRange(int port, String address, byte[] rangeFrom, byte[] rangeTo) {
        this.port = port;
        this.address = address;
        this.rangeFrom = rangeFrom;
        this.rangeTo = rangeTo;
    }

    public int getPort() {
        return this.port;
    }

    public String getAddress() {
        return this.address;
    }

    public byte[] getRangeFrom() {
        return this.rangeFrom;
    }

    public byte[] getRangeTo() {
        return this.rangeTo;
    }

    public boolean withinKeyRange(byte[] keyBytes) {

        BigInteger start = new BigInteger(1, rangeFrom);
        BigInteger end = new BigInteger(1, rangeTo);
        BigInteger key = new BigInteger(1, keyBytes);

        if (start.compareTo(end) <= 0) {
            return key.compareTo(start) <= 0 || key.compareTo(end) > 0; 
        } else {
            return key.compareTo(start) <= 0 && key.compareTo(end) > 0;
        }

    }
}
