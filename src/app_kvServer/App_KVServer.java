package app_kvServer;

import java.io.IOException;

import org.apache.log4j.Level;

import logger.LogSetup;

public class App_KVServer {

    public static void main(String[] args) {

        try {
            new LogSetup("logs/server.log", Level.ALL);	
            if (args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port>!");
            } else {
                int port = Integer.parseInt(args[0]);
                new KVServer(port, 100, "LRU").start();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        }

    }

}
