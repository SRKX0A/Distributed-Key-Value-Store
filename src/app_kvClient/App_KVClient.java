package app_kvClient;

import java.io.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

public class App_KVClient extends Thread {

    public static Logger logger = Logger.getRootLogger();
    private KVClient client;

    boolean running;

    public App_KVClient() {
	this.client = new KVClient();
	this.running = true;
    }

    public void start() {
	
	try {
	    this.client.newConnection("127.0.0.1", 50000);
	    while(this.running) {
		this.client.put("test_key", "test_value");
		Thread.sleep(4000);
	    }
	} catch (Exception e) {
	    logger.error(e.toString());
	}

    }

    public static void main(String[] args) {
	try {
	    new LogSetup("logs/client.log", Level.ALL);
	    new App_KVClient().start();
	} catch (IOException e) {
	    System.out.println("Error! Unable to initialize logger!");
	    e.printStackTrace();
	    System.exit(1);
	}
    }

}
