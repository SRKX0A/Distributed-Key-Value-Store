package testing;

import java.io.*;
import java.net.*;
import java.util.Arrays;

import app_kvServer.KVServer;
import app_kvClient.KVClient;
import client.KVStore;
import client.ProtocolMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import org.junit.Test;

import junit.framework.TestCase;


public class ClientTest extends TestCase {

    private KVClient kvClient;
    private KVServer kvServer;

    public void setUp() {

	try {
	    (new File("src/testing/data")).mkdirs();
	    (new File("src/testing/data/wal.txt")).createNewFile();
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

	try {
	    kvServer = new KVServer("localhost", 0, "src/testing/data", 3);
	    kvServer.start();
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

	while (!kvServer.isOnline() && !kvServer.isFinished());

	kvClient = new KVClient();

    }

    public void tearDown() {

	kvClient.disconnect();
	kvServer.close();
	
	File[] testing_data = (new File("src/testing/data")).listFiles();

	for (File f: testing_data) {
	    f.delete();
	}

    }


    private ProtocolMessage receiveMessage(InputStream input) throws Exception {

	    int byteCount = 0;
	    int index = 0;
	    byte[] msgBuf = new byte[1024];

	    byte prev_value = 0;
	    byte cur_value = 0;

	    while ((cur_value = (byte) input.read()) != -1) {

		msgBuf[byteCount++] = cur_value;
		index++;

		if (byteCount > 120 * 1024) {
		    break;
		}

		if (prev_value == 13 && cur_value == 10) {
		    break;
		}

		if (index == 1024) {
		    byte[] tmpBuf = new byte[1024 + byteCount];
		    System.arraycopy(msgBuf, 0, tmpBuf, 0, byteCount);
		    msgBuf = tmpBuf;
		    index = 0;
		}

		prev_value = cur_value;

	    }

	    byte[] tmpBuf = new byte[byteCount];
	    System.arraycopy(msgBuf, 0, tmpBuf, 0, byteCount);
	    msgBuf = tmpBuf;

	    return ProtocolMessage.fromBytesAtClient(msgBuf);
    }


    @Test
    public void testClientConnection() {
	KVMessage response = null;
	Exception ex = null;

	try {
	    kvClient.newConnection("localhost", kvServer.getPort());
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null);
    }

    @Test
    public void testClientKeyTooLong() {
	Exception ex = null;

	try {
	    kvClient.newConnection("localhost", kvServer.getPort());
	    kvClient.put("11111111111111111111111111111111111111111111", "value");
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex instanceof IllegalArgumentException);
    }

    @Test
    public void testClientValueTooLong() {
	Exception ex = null;

	StringBuilder sb = new StringBuilder();

	for (int i = 0; i < 124*1024; i++) {
	    sb.append('a');
	}

	try {
	    kvClient.newConnection("localhost", kvServer.getPort());
	    kvClient.put("test", sb.toString());
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex instanceof IllegalArgumentException);
    }

}
