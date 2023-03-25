package testing;

import java.io.*;
import java.net.*;
import java.util.*;

import app_kvECS.ECS;
import app_kvServer.KVServer;
import app_kvClient.KVClient;
import client.KVStore;
import client.ProtocolMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import org.junit.Test;

import junit.framework.TestCase;


public class ClientRandomTest extends TestCase {

    private KVStore kvStore1;
    private KVStore kvStore2;
    private KVStore kvStore3;
    private KVStore kvStore4;
    private KVServer kvServer1;
    private KVServer kvServer2;
    private KVServer kvServer3;
    private KVServer kvServer4;
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
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
        (new File("src/testing/data")).mkdirs();
        (new File("src/testing/data/data1")).mkdirs();
        (new File("src/testing/data/data1/wal.txt")).createNewFile();
        (new File("src/testing/data/data2")).mkdirs();
        (new File("src/testing/data/data2/wal.txt")).createNewFile();
        (new File("src/testing/data/data3")).mkdirs();
        (new File("src/testing/data/data3/wal.txt")).createNewFile();
        (new File("src/testing/data/data4")).mkdirs();
        (new File("src/testing/data/data4/wal.txt")).createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            kvServer1 = new KVServer("localhost", 5001, "localhost", ecs.getPort(), "src/testing/data/data1", 3, 500L);
            kvServer2 = new KVServer("localhost", 5002, "localhost", ecs.getPort(), "src/testing/data/data2", 3, 500L);
            kvServer3 = new KVServer("localhost", 5003, "localhost", ecs.getPort(), "src/testing/data/data3", 3, 500L);
            kvServer4 = new KVServer("localhost", 5004, "localhost", ecs.getPort(), "src/testing/data/data4", 3, 20000L);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        kvServer1.start();
        kvServer2.start();
        kvServer3.start();
        kvServer4.start();

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        kvStore1 = new KVStore("localhost", kvServer1.getPort());
        kvStore2 = new KVStore("localhost", kvServer2.getPort());
        kvStore3 = new KVStore("localhost", kvServer3.getPort());
        kvStore4 = new KVStore("localhost", kvServer4.getPort());

    }

    public void tearDown() {

        kvStore1.disconnect();
	kvStore2.disconnect();
	kvStore3.disconnect();
	kvStore4.disconnect();
        kvServer4.close();
        kvServer3.close();
        kvServer2.close();
        kvServer1.close();
        ecs.close();

        File data1 = new File("src/testing/data/data1");	

        for (File f: data1.listFiles()) {
            f.delete();
        }

        data1.delete();

        File data2 = new File("src/testing/data/data2");	

        for (File f: data2.listFiles()) {
            f.delete();
        }

        data2.delete();

        File data3 = new File("src/testing/data/data3");	

        for (File f: data3.listFiles()) {
            f.delete();
        }

        data3.delete();

        File data4 = new File("src/testing/data/data4");	

        for (File f: data4.listFiles()) {
            f.delete();
        }

        data4.delete();

        try {
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    @Test
    public void testClientDeterministicPut() {
        Exception ex = null;

        try {
            kvStore1.connect();
	    kvStore1.put("kkkvvvkk", "value4");
	    Thread.sleep(1000);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

	File data1 = new File("src/testing/data/data1");

	boolean foundKVFile = false;

	for (File f: data1.listFiles()) {
	    if (f.getName().startsWith("KVServerStoreFile_")) {
		foundKVFile = true;
	    }
	}

	assertFalse(foundKVFile);

	File data2 = new File("src/testing/data/data2");

	foundKVFile = false;

	for (File f: data2.listFiles()) {
	    if (f.getName().startsWith("KVServerStoreFile_")) {
		foundKVFile = true;
	    }
	}

	assertFalse(foundKVFile);

	File data3 = new File("src/testing/data/data3");

	foundKVFile = false;

	for (File f: data3.listFiles()) {
	    if (f.getName().startsWith("KVServerStoreFile_")) {
		foundKVFile = true;
	    }
	}

	assertFalse(foundKVFile);
	
	File data4 = new File("src/testing/data/data4");

	foundKVFile = false;

	for (File f: data4.listFiles()) {
	    if (f.getName().startsWith("KVServerStoreFile_")) {
		foundKVFile = true;
	    }
	}

	assertTrue(foundKVFile);

    }

    @Test
    public void testClientRandomGetAfterReplication() {
        Exception ex = null;

	KVMessage reply = null;

        try {
            kvStore1.connect();
            kvStore1.put("key", "value");
	    Thread.sleep(1000);
	    kvStore2.connect();
	    reply = kvStore2.get("key");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

	assertEquals(reply.getValue(), "value");

    }

    @Test
    public void testClientRandomGetNoReplication() {
        Exception ex = null;

	KVMessage reply = null;

        try {
            kvStore4.connect();
	    kvStore3.connect();
	    kvStore4.put("kkkvvvkk", "value4");
	    reply = kvStore3.get("kkkvvvkk");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

	assertEquals(reply.getStatus(), StatusType.GET_ERROR);

    }

}
