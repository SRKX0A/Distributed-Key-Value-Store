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


public class ReplicationTest extends TestCase {

    private KVStore kvStore1;
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
            kvServer4 = new KVServer("localhost", 5004, "localhost", ecs.getPort(), "src/testing/data/data4", 3, 500L);
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

    }

    public void tearDown() {

        kvStore1.disconnect();
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
    public void testSingleServerNoReplication() {
        Exception ex = null;

        kvServer4.close();
        kvServer3.close();
        kvServer2.close();

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            kvStore1.connect();
            kvStore1.put("key", "value");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

	File data1 = new File("src/testing/data/data1");

	for (File f: data1.listFiles()) {
	    assertFalse(f.getName().startsWith("Replica"));
	}

    }

    @Test
    public void testTwoServersCircularReplication() {

        Exception ex = null;

        kvServer4.close();
        kvServer3.close();

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            kvStore1.connect();
            kvStore1.put("key1", "value1");
	    kvStore1.put("key2", "value2");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

	File data1 = new File("src/testing/data/data1");

	boolean foundReplica1File = false;

	for (File f: data1.listFiles()) {
	    if (f.getName().startsWith("Replica")) {
		foundReplica1File = true;
		assertTrue(f.getName().startsWith("Replica1"));
	    }
	}

	assertTrue(foundReplica1File);

	File data2 = new File("src/testing/data/data2");

	foundReplica1File = false;

	for (File f: data2.listFiles()) {
	    if (f.getName().startsWith("Replica")) {
		foundReplica1File = true;
		assertTrue(f.getName().startsWith("Replica1"));
	    }
	}

	assertTrue(foundReplica1File);

    }

    @Test
    public void testMultipleServersDistinctReplication() {

        Exception ex = null;

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            kvStore1.connect();
            kvStore1.put("aaaa", "value1");
	    kvStore1.put("bbbb", "value2");
	    kvStore1.put("kkkkk", "value3");
	    kvStore1.put("kkkvvvkk", "value4");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

	File data1 = new File("src/testing/data/data1");

	boolean foundReplica1File = false;
	boolean foundReplica2File = false;

	for (File f: data1.listFiles()) {
	    if (f.getName().startsWith("Replica1")) {
		foundReplica1File = true;
	    } else if (f.getName().startsWith("Replica2")) {
		foundReplica2File = true;
	    }
	}

	assertTrue(foundReplica1File);
	assertTrue(foundReplica2File);

	File data2 = new File("src/testing/data/data2");

	foundReplica1File = false;
	foundReplica2File = false;

	for (File f: data2.listFiles()) {
	    if (f.getName().startsWith("Replica1")) {
		foundReplica1File = true;
	    } else if (f.getName().startsWith("Replica2")) {
		foundReplica2File = true;
	    }
	}

	assertTrue(foundReplica1File);
	assertTrue(foundReplica2File);

	File data3 = new File("src/testing/data/data3");

	foundReplica1File = false;
	foundReplica2File = false;

	for (File f: data3.listFiles()) {
	    if (f.getName().startsWith("Replica1")) {
		foundReplica1File = true;
	    } else if (f.getName().startsWith("Replica2")) {
		foundReplica2File = true;
	    }
	}

	assertTrue(foundReplica1File);
	assertTrue(foundReplica2File);

    }

}
