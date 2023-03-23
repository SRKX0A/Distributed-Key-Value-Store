package testing;

import java.io.*;
import java.net.*;
import java.util.Arrays;

import app_kvECS.ECS;
import app_kvServer.KVServer;
import app_kvClient.KVClient;
import client.KVStore;
import client.ProtocolMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import org.junit.Test;

import junit.framework.TestCase;


public class StoreTest extends TestCase {

    private KVStore kvStore;
    private KVServer kvServer1;
    private KVServer kvServer2;
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
	    kvServer1 = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data", 3, 5000L);
	    kvServer2 = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data", 3, 5000L);
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

	kvServer1.start();
	kvServer2.start();

	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

    }

    public void tearDown() {

	kvStore.disconnect();
	kvServer2.close();
	kvServer1.close();
	ecs.close();
	
	File[] testing_data = (new File("src/testing/data")).listFiles();

	for (File f: testing_data) {
	    f.delete();
	}

	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}
    }

    @Test
    public void testStoreSingleConnection() {
	Exception ex = null;

	kvStore = new KVStore("localhost", kvServer1.getPort());

	try {
	    kvStore.connect();
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);
    }

    @Test
    public void testStoreNoRedirectConnection() {
	Exception ex = null;
	KVMessage message = null;

	kvStore = new KVStore("localhost", kvServer1.getPort());

	try {
	    kvStore.connect();
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);

	try {
	    message = kvStore.put("test_key", "test_value");
	} catch (Exception e) {
	    ex = e; 
	}

	assertNull(ex);

	assertTrue(message.getStatus() == StatusType.PUT_SUCCESS);
    }

    @Test
    public void testNoRedirectConnection() {
	Exception ex = null;
	KVMessage message = null;

	kvStore = new KVStore("localhost", kvServer2.getPort());

	try {
	    kvStore.connect();
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);

	try {
	    message = kvStore.put("test_key", "test_value");
	} catch (Exception e) {
	    ex = e; 
	}

	assertNull(ex);

	assertTrue(message.getStatus() == StatusType.PUT_SUCCESS);
    }

}
