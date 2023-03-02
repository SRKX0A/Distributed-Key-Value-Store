package testing;

import java.io.*;
import java.net.*;
import java.text.MessageFormat;
import java.util.Random;

import app_kvECS.ECS;
import app_kvServer.KVServer;
import client.KVStore;
import client.ProtocolMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import org.junit.Test;

import junit.framework.TestCase;


public class PerformanceTest extends TestCase {

    private KVStore kvClient;
    private KVServer kvServer;
    private ECS ecs;
    private float iterations = 1000;

    private Random random = new Random();

    public void setUp() {

	try {
	    ecs = new ECS("localhost", 0); 
	    ecs.start();
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

        removeFiles();
        try {
            (new File("src/testing/data")).mkdirs();
            (new File("src/testing/data/wal.txt")).createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            kvServer = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data", 10);
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

        kvClient = new KVStore("localhost", kvServer.getPort());

	try {
	    kvClient.connect();
	} catch (Exception e) {
	    e.printStackTrace();
	    return;
	}
    }

    public void tearDown() {

        kvClient.disconnect();
        kvServer.close();
	ecs.close();
        //removeFiles();

    }

    public void removeFiles()
    {
        File[] testing_data = (new File("src/testing/data")).listFiles();

        for (File f: testing_data) {
            f.delete();
        }
    }

    // 50% get 50% Put
    @Test
    public void testPeformace()
    {
        runLatencyTest(1,1,"bar", false);
        runLatencyTest(1,1,"barUpdate", false);
        // removeFiles();
        // runLatencyTest(1,1,"bar", true);
        // removeFiles();
        // runLatencyTest(4, 1, "bar", false);
        // removeFiles();
        // runLatencyTest(4, 1, "bar", true);
        // removeFiles();
        // runLatencyTest(1,4,"bar", false);
        // removeFiles();
        // runLatencyTest(1,4,"bar", true);
    }

    public void runLatencyTest(int putNumber, int getNumber, String valueString, boolean getError){
        Exception ex = null;
        try{

            int putCount = putNumber;
            int getCount = getNumber;

            int count = 0;
            String key = "foo" +  String.valueOf(count);
            String value = valueString + String.valueOf(count);

            String mostRecentKey = "";
            float totalPut = 0;
            float totalGet = 0;
            float totalErrorGet = 0;

            long start = System.currentTimeMillis();
            float totalGetTime = 0;
            float totalPutTime = 0;
            while (count < iterations)
            {
                
                if (putCount != 0)
                {
                    long putStart = System.currentTimeMillis();
                    kvClient.put(key, value);
                    float putElapsedTime =  System.currentTimeMillis() - putStart;
                    totalPutTime = totalPutTime + putElapsedTime;

                    mostRecentKey = key;
                    key = "foo" + String.valueOf(count + 1);
                    value = valueString +   String.valueOf(count + 1);

                    putCount = putCount - 1;
                    getCount = getNumber;
                    totalPut = totalPut + 1;
                }
                else
                {
                    String keyVal = mostRecentKey;
                    if (getError == true && random.nextBoolean())
                    {
                        keyVal = "ErrorKey";
                        totalErrorGet = totalErrorGet + 1;
                    }
                    long getStart = System.currentTimeMillis();
                    KVMessage getResult =kvClient.get(keyVal);
                    //System.out.println(mostRecentKey + " " + getResult.getValue());

                    float getElapsedTime =  System.currentTimeMillis() - getStart;
                    totalGetTime = totalGetTime + getElapsedTime;

                    getCount = getCount - 1;
                    totalGet = totalGet + 1;
                    if (getCount == 0)
                        putCount = putNumber;
                }
                count = count + 1;
            }

            float elapsedTime = System.currentTimeMillis() - start;
            String message = MessageFormat.format("{0} iterations of {1}% Put request and {2}% Get requests has taken {3}ms for an average of {4}ms per request. \n Total Get Time {5}ms with an average of {6}ms per request with error rate of {7}% \n  Total Put Time {8}ms with an average of {9}ms per request \n", 
            String.valueOf(iterations), String.valueOf((float)((float)(totalPut/iterations) * 100)), String.valueOf((float)((float)(totalGet/iterations) * 100)),
            String.valueOf(elapsedTime), String.valueOf(elapsedTime/iterations), String.valueOf(totalGetTime), String.valueOf(totalGetTime/totalGet), String.valueOf((float)((float)(totalErrorGet/totalGet) * 100)),
            String.valueOf(totalPutTime), String.valueOf(totalPutTime/totalPut));
            System.out.println(message);

        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
            return;
        }

        assertTrue(ex == null);
    }

}
