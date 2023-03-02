package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

    static {
	try {
	    new LogSetup("logs/testing/test.log", Level.OFF);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    
    
    public static Test suite() {
	TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
	clientSuite.addTestSuite(ProtocolTest.class);
	clientSuite.addTestSuite(ClientTest.class);
	clientSuite.addTestSuite(ServerTest.class);
	clientSuite.addTestSuite(PerformanceTest.class);
	return clientSuite;
    }
	
}
