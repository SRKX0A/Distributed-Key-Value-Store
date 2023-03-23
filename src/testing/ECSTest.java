package testing;

import java.io.*;

import app_kvECS.ECS;
import app_kvServer.KVServer;

import org.junit.Test;

import junit.framework.TestCase;

public class ECSTest extends TestCase {

    public void setUp() {

	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}

    }

    public void tearDown() {

	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}
    }

    @Test
    public void testECSCreation() {

	Exception ex = null;
	ECS ecs = null;

	try {
	    ecs = new ECS("localhost", 0); 
	    ecs.start();
	} catch (Exception e) {
	    ex = e;
	} finally {
	    ecs.close();
	}

	assertNull(ex);
    }

    @Test
    public void testECSSingleConnection() {

	Exception ex = null;
	ECS ecs = null;
	KVServer kvServer = null;

	try {
	    ecs = new ECS("localhost", 0); 
	    ecs.start();
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);

	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);
	
	try {
	    (new File("src/testing/data")).mkdirs();
	    (new File("src/testing/data/wal.txt")).createNewFile();
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);

	try {
	    kvServer = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data", 3, 5000L);
	    kvServer.start();

	    try {
		Thread.sleep(500);
	    } catch (Exception e) {
		ex = e;
	    }

	    assertNull(ex);
	} catch (Exception e) {
	    ex = e;
	} finally {
	    kvServer.close();
	    ecs.close();
	}

	assertNull(ex);

	File[] testing_data = (new File("src/testing/data")).listFiles();

	for (File f: testing_data) {
	    f.delete();
	}
    }

    @Test
    public void testECSMultipleConnections() {

	Exception ex = null;
	ECS ecs = null;
	KVServer kvServer1 = null;
	KVServer kvServer2 = null;

	try {
	    ecs = new ECS("localhost", 0); 
	    ecs.start();
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);

	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);
	
	try {
	    (new File("src/testing/data")).mkdirs();
	    (new File("src/testing/data/wal.txt")).createNewFile();
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);

	try {
	    kvServer1 = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data", 3, 5000L);
	    kvServer2 = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data", 3, 5000L);
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);

	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);

	kvServer1.start();
	kvServer2.start();
	
	try {
	    Thread.sleep(500);
	} catch (Exception e) {
	    ex = e;
	}

	assertNull(ex);

	kvServer2.close();
	kvServer1.close();
	ecs.close();

	File[] testing_data = (new File("src/testing/data")).listFiles();

	for (File f: testing_data) {
	    f.delete();
	}
    }

}
