package testing;

import java.io.*;
import java.net.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import app_kvECS.ECS;
import app_kvServer.KVServer;
import client.KVStore;
import client.ProtocolMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import org.junit.Test;

import app_kvClient.KVClient;
import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import logger.LogSetup;

public class PerformanceTestM2 extends TestCase {

    private KVStore kvClient;
    private KVServer kvServer;
    private ECS ecs;
    private float iterations = 3000;
    private int numServers = 1;
    private int numClients = 5;

    private List<KVStore> clients = new ArrayList<KVStore>();
    private List<KVServer> servers = new ArrayList<KVServer>();

    private Random random = new Random();
    private float totalElapsedTime = 0;
    private float totalThroughput = 0;
    private float throughputTime = 100;
    private float numRequests = 25;

    private int cacheSize = 20;

    public static Logger logger = Logger.getRootLogger();

    public void setUp() {

        try {

            new LogSetup("logs/testing/test.log", Level.FATAL);

            System.out.println("Connecting to ECS at port 0");
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

        try {

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            kvServer = new KVServer("localhost", 0, "localhost", ecs.getPort(), "src/testing/data/0", cacheSize, 5000L);
            kvServer.start();
            System.out.println("Starting Server with port:" + String.valueOf(kvServer.getPort())
                    + " and ECS server at port: " + String.valueOf(ecs.getPort()));
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
        for (KVStore kvStore : clients) {
            kvStore.disconnect();
        }

        kvServer.close();
        ecs.close();

    }

    public void tempTearDown() {
        for (KVStore kvStore : clients) {
            kvStore.disconnect();
        }

        clients.clear();

        try {
            long start = System.currentTimeMillis();
            for (KVServer server : servers) {
                server.close();
                Thread.sleep(1000);
            }
            float elapsedTime = System.currentTimeMillis() - start;
            servers.clear();

            System.out.println("It took an average of " + String.valueOf(elapsedTime / (float) (numServers - 1))
                    + " ms to remove a server from a " + String.valueOf(numServers - 1) + " System");

            logger.fatal("It took an average of " + String.valueOf(elapsedTime / (float) (numServers - 1))
                    + " ms to remove a server from a " + String.valueOf(numServers - 1) + " System");

            System.out.println(" ");

            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    public void removeFiles(String folderName) {
        File[] testing_data = (new File(folderName)).listFiles();
        if (testing_data != null) {
            for (File f : testing_data) {
                f.delete();
            }
        }
    }

    public void runTest(int numOfServers, int numOfClients) {
        logger.fatal("Running Test for Servers:" + numOfServers + " Clients:" + numOfClients);
        numServers = numOfServers;
        numClients = numOfClients;

        createServers(numServers);
        createClients(numClients);
        performClientGet();

        System.out.println(clients.size() + " Clients with " + String.valueOf(servers.size() + 1)+ " Servers, took an average of "
                + String.valueOf(totalElapsedTime / (float) numClients)
                + " ms to process " + numRequests + " Requests");

        System.out.println(
                String.valueOf(clients.size()) + " Clients with " + String.valueOf(servers.size() + 1) + " Servers, had an average of "
                        + String.valueOf(totalThroughput / (float) numClients)
                        + " requests per " + throughputTime + " ms");

        logger.fatal(clients.size() + " Clients with " + String.valueOf(servers.size() + 1) + " Servers, took an average of "
                + String.valueOf(totalElapsedTime / (float) numClients)
                + " ms to process " + numRequests + " Requests");

        logger.fatal(
                String.valueOf(clients.size()) + " Clients with " + String.valueOf(servers.size() + 1) + " Servers, had an average of "
                        + String.valueOf(totalThroughput / (float) numClients)
                        + " requests per " + throughputTime + " ms");

        System.out.println(" ");

        totalElapsedTime = 0;
        totalThroughput = 0;

    }

    public void removeClients() {
        for (KVStore kvStore : clients) {
            kvStore.disconnect();
        }

        clients.clear();

        assertTrue(true);
    }

    // 50% get 50% Put
    @Test
    public void testPeformace() {

        populateData(1, 1, "My Name and Number is ", false);
        // runTest(1, 1);
        // runTest(1, 5);
        // runTest(1, 20);
        // runTest(1, 50);
        // runTest(1, 100);

        // removeClients();

        // runTest(5, 1);
        // runTest(5, 5);
        // runTest(5, 20);
        // runTest(5, 50);
        // runTest(5, 100);

        // removeClients();

        // runTest(10, 1);
        // runTest(10, 5);
        // runTest(10, 20);
        // runTest(10, 50);
        // runTest(10, 100);

        // removeClients();

        // runTest(50, 1);
        // runTest(50, 5);
        runTest(50, 20);
        // runTest(50, 50);
        // runTest(50, 100);

        // removeClients();

        // runTest(100, 1);
        // runTest(100, 5);
        // runTest(100, 20);
        // runTest(100, 50);
        // runTest(100, 100);

        // removeClients();

    }

    public void populateData(int putNumber, int getNumber, String valueString, boolean getError) {
        Exception ex = null;
        logger.fatal("Starting Populating Data");
        try {
            removeFiles("src/testing/data/0");
            (new File("src/testing/data/0")).mkdirs();
            (new File("src/testing/data/0/wal.txt")).createNewFile();

            int count = 0;
            String key = String.valueOf(count) + "foo";
            String value = UUID.randomUUID().toString();

            while (count < iterations) {

                kvClient.put(key, value);
                key = String.valueOf(count + 1) + "foo";
                value = UUID.randomUUID().toString();
                count = count + 1;
            }

            System.out.println("Finished Populating data with " + String.valueOf(iterations) + " Key Value Pairs");
            System.out.println(" ");

        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
            return;
        }

        logger.fatal("Finished Populating Data");
        assertTrue(ex == null);
    }

    public void createServers(int NumberOfServers) {
        Exception ex = null;
        try {
            int startServerCount = servers.size() + 1;
            for (int i = startServerCount; i < NumberOfServers; i++) {
                String folderName = "src/testing/data/" + String.valueOf(i);
                removeFiles(folderName);
                (new File(folderName)).mkdirs();
                (new File(folderName + "/wal.txt")).createNewFile();

                KVServer tempServer = new KVServer("localhost", 0, "localhost", ecs.getPort(), folderName, cacheSize, 5000L);
                servers.add(tempServer);
                tempServer.start();
                // System.out.println("Starting Server with port:" +
                // String.valueOf(tempServer.getPort())
                // + " and ECS server at port: " + String.valueOf(ecs.getPort()));

                Thread.sleep(2000);
            }
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
            return;
        }

        assertTrue(ex == null);
    }

    public void createClients(int numberOfClients) {
        Exception ex = null;
        try {
            int startClientCount = clients.size();
            for (int i = startClientCount; i < numberOfClients; i++) {

                if (servers.size() == 0) {
                    KVStore client = new KVStore("localhost", kvServer.getPort());
                    clients.add(client);
                    client.connect();
                } else {
                    int rnd = new Random().nextInt(servers.size());
                    KVServer server = servers.get(rnd);
                    KVStore client = new KVStore("localhost", server.getPort());
                    clients.add(client);
                    client.connect();
                }
            }
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
            return;
        }

        assertTrue(ex == null);
    }

    public void performClientGet() {
        Exception ex = null;
        try {
            for (KVStore kvStore : clients) {
                clientGet(kvStore);
            }
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
            return;
        }

        assertTrue(ex == null);
    }

    public void clientGet(KVStore client) {

        Exception ex = null;
        boolean throughputCheck = false;
        try {
            int count = 0;
            String key = String.valueOf(count) + "foo";

            long start = System.currentTimeMillis();

            Random rand = new Random();

            while (count < numRequests) {
                KVMessage message = client.get(key);

                int randomNumber = rand.nextInt((int) iterations - 1);
                key = String.valueOf(randomNumber) + "foo";

                float tempElapsedTime = System.currentTimeMillis() - start;
                if (tempElapsedTime > throughputTime && throughputCheck == false) {
                    throughputCheck = true;
                    totalThroughput = totalThroughput + count;
                }

                count = count + 1;
            }
            float elapsedTime = System.currentTimeMillis() - start;
            totalElapsedTime = totalElapsedTime + elapsedTime;

            if (throughputCheck == false) {
                totalThroughput = totalThroughput + count;
            }

            // System.out.println(String.valueOf(numRequests) + " GETS took " +
            // String.valueOf(elapsedTime)
            // + "ms, for Client " + String.valueOf(clients.indexOf(client)));

        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
            return;
        }

        assertTrue(ex == null);
    }

}
