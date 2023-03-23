package testing;

import java.io.*;
import java.net.*;

import app_kvECS.ECS;
import app_kvServer.KVServer;
import client.KVStore;
import client.ProtocolMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import org.junit.Test;

import junit.framework.TestCase;


public class ServerTest extends TestCase {

    private KVStore kvClient1;
    private KVStore kvClient2;
    private KVServer kvServer;
    private ECS ecs;

    public void setUp() {

	try {
	    ecs = new ECS("localhost", 0); 
	    ecs.start();
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

	try {
	    (new File("src/testing/data")).mkdirs();
	    (new File("src/testing/data/wal.txt")).createNewFile();
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

	try {
	    kvServer = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data", 3, 5000L);
	    kvServer.start();
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

	kvClient1 = new KVStore("localhost", kvServer.getPort());
	kvClient2 = new KVStore("localhost", kvServer.getPort());

	try {
	    kvClient1.connect();
	    kvClient2.connect();
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

    }

    public void tearDown() {

	kvClient1.disconnect();
	kvClient2.disconnect();
	kvServer.close();
	ecs.close();
	
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
    public void testInCachePut() {
	KVMessage response = null;
	Exception ex = null;

	try {
	    response = kvClient1.put("foo1", "bar1");
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS && kvServer.inCache("foo1") == true);
    }

    @Test
    public void testInStoragePut() {

	KVMessage response1 = null;
	KVMessage response2 = null;
	KVMessage response3 = null;
	Exception exc = null;
	Exception exs = null;

	try {
	    response1 = kvClient1.put("foo1", "bar1");
	    response2 = kvClient1.put("foo2", "bar2");
	    response3 = kvClient1.put("foo3", "bar3");
	} catch (Exception e) {
	    exc = e;
	}

	boolean foo1InStorage = false;
	boolean foo2InStorage = false;
	boolean foo3InStorage = false;

	try {
	    foo1InStorage = kvServer.inStorage("foo1"); 
	    foo2InStorage = kvServer.inStorage("foo2"); 
	    foo3InStorage = kvServer.inStorage("foo3"); 
	} catch (Exception e) {
	    exs = e;
	}

	assertTrue(
	    exc == null && 
	    exs == null && 
	    response1.getStatus() == StatusType.PUT_SUCCESS && 
	    response2.getStatus() == StatusType.PUT_SUCCESS && 
	    response3.getStatus() == StatusType.PUT_SUCCESS && 
	    foo1InStorage == true &&
	    foo2InStorage == true &&
	    foo3InStorage == true &&
	    kvServer.inCache("foo1") == false &&
	    kvServer.inCache("foo2") == false &&
	    kvServer.inCache("foo3") == false
	);
    }

    @Test
    public void testMalformedMessage() {
	KVMessage response = null;
	Exception ex = null;

	try {

	    byte[] malformedMessage = (new String("malformedmessage\r\n")).getBytes("UTF-8");
	    Socket socket = new Socket("localhost", kvServer.getPort());
	    InputStream input = socket.getInputStream();
	    OutputStream output = socket.getOutputStream();

	    output.write(malformedMessage);
	    output.flush();

	    response = this.receiveMessage(input);

	    input.close();
	    output.close();
	    socket.close();

	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && response.getStatus() == StatusType.FAILED);
    }

    @Test
    public void testKeyTooLong() {
	KVMessage response = null;
	Exception ex = null;

	try {

	    byte[] keyTooLongMessage = (new String("put 1111111111111111111111111111111111111111 test\r\n")).getBytes("UTF-8");
	    Socket socket = new Socket("localhost", kvServer.getPort());
	    InputStream input = socket.getInputStream();
	    OutputStream output = socket.getOutputStream();

	    output.write(keyTooLongMessage);
	    output.flush();

	    response = this.receiveMessage(input);

	    input.close();
	    output.close();
	    socket.close();

	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && response.getStatus() == StatusType.FAILED);
    }

    @Test
    public void testValueTooLong() {
	KVMessage response = null;
	Exception ex = null;

	try {

	    StringBuilder sb = new StringBuilder();

	    for (int i = 0; i < 124*1024; i++) {
		sb.append('a');
	    }

	    String msg = "put test " + sb.toString() + "\r\n";

	    byte[] valueTooLongMessage = msg.getBytes("UTF-8");
	    Socket socket = new Socket("localhost", kvServer.getPort());
	    InputStream input = socket.getInputStream();
	    OutputStream output = socket.getOutputStream();

	    output.write(valueTooLongMessage);
	    output.flush();

	    response = this.receiveMessage(input);

	    input.close();
	    output.close();
	    socket.close();

	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && response.getStatus() == StatusType.FAILED);
    }

    @Test
    public void testMultipleClientsSameKey() {
	KVMessage response1 = null;
	KVMessage response2 = null;
	KVMessage response3 = null;
	KVMessage response4 = null;
	Exception ex = null;

	try {
	    response1 = kvClient1.put("test", "value1");
	    response2 = kvClient2.put("test", "value2");
	    response3 = kvClient1.get("test");
	    response4 = kvClient1.get("test");
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null &&
	    response1.getStatus() == StatusType.PUT_SUCCESS &&
	    response2.getStatus() == StatusType.PUT_UPDATE &&
	    response3.getStatus() == StatusType.GET_SUCCESS &&
	    response4.getStatus() == StatusType.GET_SUCCESS &&
	    response3.getValue().equals("value2") &&
	    response4.getValue().equals("value2")
	);
    }

    @Test
    public void testMultipleClientsDeleteKey() {
	KVMessage response1 = null;
	KVMessage response2 = null;
	KVMessage response3 = null;
	KVMessage response4 = null;
	Exception ex = null;

	try {
	    response1 = kvClient1.put("test", "value1");
	    response2 = kvClient2.put("test", "null");
	    response3 = kvClient1.get("test");
	    response4 = kvClient1.get("test");
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null &&
	    response1.getStatus() == StatusType.PUT_SUCCESS &&
	    response2.getStatus() == StatusType.PUT_SUCCESS &&
	    response3.getStatus() == StatusType.GET_ERROR &&
	    response4.getStatus() == StatusType.GET_ERROR &&
	    response3.getValue().equals("null") &&
	    response4.getValue().equals("null")
	);
    }
}
