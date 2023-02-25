package shared;

import java.io.*;
import java.util.*;

public class ByteArrayComparator implements Serializable, Comparator<byte[]> {

    public int compare(byte[] b1, byte[] b2) {
	for (int i = 0; i < b1.length; i++) {
	if (b1[i] < b2[i]) {
		return -1;
	    } else if (b1[i] > b2[i]) {
		return 1;
	    }
	}

	return 0;
    }

}
