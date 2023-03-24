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


public class KeyrangeReadTest extends TestCase {

    private KVStore kvStore1;
    private KVStore kvStore2;
    private KVStore kvStore3;
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
            kvServer1 = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data/data1", 3, 5000L);
            kvServer2 = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data/data2", 3, 5000L);
            kvServer3 = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data/data3", 3, 5000L);
            kvServer4 = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data/data4", 3, 5000L);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        kvServer1.start();
        kvServer2.start();
        kvServer3.start();
        kvServer4.start();

        try {
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        kvStore1 = new KVStore("localhost", kvServer1.getPort());
        kvStore2 = new KVStore("localhost", kvServer2.getPort());
	kvStore3 = new KVStore("localhost", kvServer3.getPort());

    }

    public void tearDown() {

        kvStore1.disconnect();
        kvStore2.disconnect();
        kvStore3.disconnect();
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
    public void testKeyrangeReadSingleServer() {
        Exception ex = null;

        kvServer4.close();
        kvServer3.close();
        kvServer2.close();

        try {
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        ProtocolMessage reply = null;

        try {
            kvStore1.connect();
            reply = kvStore1.keyrangeread();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertEquals(reply.getKey(), kvServer1.getKeyRangeReadSuccessString());
    }

    @Test
    public void testKeyrangeReadMultipleServers() {
        Exception ex = null;

        ProtocolMessage reply1 = null;
        ProtocolMessage reply2 = null;

        try {
            kvStore1.connect();
            kvStore2.connect();
            reply1 = kvStore1.keyrangeread();
            reply2 = kvStore2.keyrangeread();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertEquals(reply1.getKey(), reply2.getKey());
    }

    @Test
    public void testKeyrangeReadIdenticalStartsAndEnds() {
        Exception ex = null;

        kvServer4.close();

        try {
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        ProtocolMessage reply1 = null;

        try {
            kvStore1.connect();
            reply1 = kvStore1.keyrangeread();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        String replyKey1 = reply1.getKey();

	List<String> keyRangesFrom1 = Arrays.asList(replyKey1.split(";"));

        for (int i = 0; i < keyRangesFrom1.size(); i++) {
	    List<String> elements = Arrays.asList(keyRangesFrom1.get(i).split(","));
	    assertEquals(elements.get(0), elements.get(1));
        }

    }

    @Test
    public void testKeyrangeReadDifferedStartsAndEnds() {
        Exception ex = null;

        ProtocolMessage reply1 = null;

        try {
            kvStore1.connect();
            reply1 = kvStore1.keyrangeread();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        String replyKey1 = reply1.getKey();

	List<String> keyRangesFrom1 = Arrays.asList(replyKey1.split(";"));

        for (int i = 0; i < keyRangesFrom1.size(); i++) {
	    List<String> elements = Arrays.asList(keyRangesFrom1.get(i).split(","));
	    assertFalse(elements.get(0).equals(elements.get(1)));
        }
    }


}
