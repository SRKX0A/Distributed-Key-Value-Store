package testing;

import java.util.Arrays;

import shared.KeyRange;

import org.junit.Test;

import junit.framework.TestCase;


public class KeyRangeTest extends TestCase {

    public void setUp() {

    }

    public void tearDown() {

    }


    @Test
    public void testKeyRangeCreation() {

	String address = "localhost";
	int port = 5001;
	byte[] rangeFrom = new byte[32];
	byte[] rangeTo = new byte[32];

	for (int i = 0; i < 32; i++) {
	    rangeFrom[i] = 0;
	    rangeTo[i] = 0;
	}

	KeyRange keyRange = new KeyRange(port, address, rangeFrom, rangeTo);

	assertTrue(port == keyRange.getPort() && 
	    address.equals(keyRange.getAddress()) &&
	    Arrays.equals(rangeFrom, keyRange.getRangeFrom()) &&
	    Arrays.equals(rangeTo, keyRange.getRangeTo()));

    }

    @Test
    public void testWithinKeyRange() {

	String address = "localhost";
	int port = 5001;
	byte[] rangeFrom = new byte[32];
	byte[] rangeTo = new byte[32];
	byte[] testValue = new byte[32];

	for (int i = 0; i < 32; i++) {
	    rangeFrom[i] = 8;
	    rangeTo[i] = 0;
	    testValue[i] = 7;
	}

	KeyRange keyRange = new KeyRange(port, address, rangeFrom, rangeTo);

	assertTrue(keyRange.withinKeyRange(testValue)); 

    }

    @Test
    public void testNotWithinKeyRange() {

	String address = "localhost";
	int port = 5001;
	byte[] rangeFrom = new byte[32];
	byte[] rangeTo = new byte[32];
	byte[] testValue = new byte[32];

	for (int i = 0; i < 32; i++) {
	    rangeFrom[i] = 8;
	    rangeTo[i] = 0;
	    testValue[i] = 9;
	}

	KeyRange keyRange = new KeyRange(port, address, rangeFrom, rangeTo);

	assertTrue(!keyRange.withinKeyRange(testValue)); 

    }

    @Test
    public void testWrapAroundKeyRange() {

	String address = "localhost";
	int port = 5001;
	byte[] rangeFrom = new byte[32];
	byte[] rangeTo = new byte[32];
	byte[] testValue = new byte[32];

	for (int i = 0; i < 32; i++) {
	    rangeFrom[i] = 8;
	    rangeTo[i] = 0xf;
	    testValue[i] = 0;
	}

	KeyRange keyRange = new KeyRange(port, address, rangeFrom, rangeTo);

	assertTrue(keyRange.withinKeyRange(testValue)); 

    }
}
