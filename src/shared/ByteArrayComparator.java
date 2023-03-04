package shared;

import java.io.*;
import java.util.*;
import java.math.*;

public class ByteArrayComparator implements Serializable, Comparator<byte[]> {

    public int compare(byte[] b1, byte[] b2) {

	BigInteger bigInt1 = new BigInteger(1, b1);
	BigInteger bigInt2 = new BigInteger(1, b2);

	return bigInt1.compareTo(bigInt2);
    }

}
