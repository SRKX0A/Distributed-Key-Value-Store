package app_kvClient;

import java.io.*;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

public class App_KVClient extends Thread {

	public static Logger logger = Logger.getRootLogger();
	private KVClient client;

	boolean running;
	boolean connected;

	public App_KVClient() {
		this.client = new KVClient();
		this.running = true;
		this.connected = false;
	}

	public void parser(String message) throws Exception {
		String[] commands = message.split(" ");

		switch (commands[0].toLowerCase()) {
			case "quit":
				shutClientDown();
				break;
			case "connect":
				connectToServer(commands);
				break;
			case "disconnect":
				disconnectFromServer();
				break;
			case "put":
				updateValue(commands);
				break;
			case "get":
				getValue(commands);
				break;
			case "help":
				helpMessage();
				break;
			case "loglevel":
				setLogLevel(commands);
				break;
			default:
				System.out.println("Unknown Command");

		}
	}

	public void setLogLevel(String[] commands) {
		if (commands.length != 2) {
			System.out.println("Invalid Number of Arguements");
			return;
		}

		switch (commands[1].toLowerCase()) {
			case "all":
				logger.setLevel(Level.ALL);
				System.out.println("Logger Level Set To: ALL");
				break;
			case "debug":
				logger.setLevel(Level.DEBUG);
				System.out.println("Logger Level Set To: DEBUG");
				break;
			case "info":
				logger.setLevel(Level.INFO);
				System.out.println("Logger Level Set To: INFO");
				break;
			case "warn":
				logger.setLevel(Level.WARN);
				System.out.println("Logger Level Set To: WARN");
				break;
			case "error":
				logger.setLevel(Level.ERROR);
				System.out.println("Logger Level Set To: ERROR");
				break;
			case "fatal":
				logger.setLevel(Level.FATAL);
				System.out.println("Logger Level Set To: FATAL");
				break;
			case "off":
				logger.setLevel(Level.OFF);
				System.out.println("Logger Level Set To: OFF");
				break;
			default:
				System.out.println("Invalid Logger Level");
		}
	}

	public void helpMessage() {
		System.out.println("To Connect To server -> connect <ServerAddress> <PortNumber>");
		System.out.println("To Disconnect -> disconnect");
		System.out.println("To Update Or Add KV Pair -> put <key> <value>");
		System.out.println("To Get KV Pair ->  get <key>");
		System.out.println("To Change LogLevel -> logLevel <level>");
		System.out.println("For Help -> help");
	}

	public void getValue(String[] commands) {
		try {
			if (!this.connected) {
				System.out.println("Client currently not connected to a server. Please Connect To A Server");
				return;
			}
			if (commands.length != 2) {
				System.out.println("Invalid Number of Arguements");
				return;
			}
			this.client.get(commands[1]);

		} catch (Exception e) {
			logger.error("FAILED: " + e.toString());
		}
	}

	public void updateValue(String[] commands) {
		try {
			if (!this.connected) {
				System.out.println("Client currently not connected to a server. Please Connect To A Server");
				return;
			}
			if (commands.length != 3) {
				System.out.println("Invalid Number of Arguements");
				return;
			}
			this.client.put(commands[1], commands[2]);

		} catch (Exception e) {
			logger.error("FAILED: " + e.toString());
		}
	}

	public void shutClientDown() throws Exception {
		if (this.connected) {
			disconnectFromServer();
		}
		System.out.println("Client Shutting Down...");
		TimeUnit.SECONDS.sleep(1);
		System.exit(1);
	}

	public void disconnectFromServer() {
		if (!this.connected) {
			System.out.println("Client currently not connected to a server");
			return;
		}
		try {
			this.client.disconnect();
			this.connected = false;
			logger.info("Client Disconnected From Server");
		} catch (Exception e) {
			logger.error("Disconnection Error: " + e.toString());
		}
	}

	public void connectToServer(String[] commands) {
		try {
			if (this.connected) {
				System.out.println(
						"Client already Connected to Server. Please Disconnect First to Connect to another Server");
				return;
			}
			if (commands.length != 3) {
				System.out.println(
						"Invalid Number of Arguements");
				return;
			}
			String serverAddress = commands[1];
			Integer serverPort = Integer.parseInt(commands[2]);
			this.client.newConnection(serverAddress, serverPort);
			String connectMessage = MessageFormat.format("Connection established with server at {0}:{1}", serverAddress,
					serverPort.toString());
			this.connected = true;
			logger.info(connectMessage);

		} catch (UnknownHostException e) {
			logger.error("Unknown Host: " + e.toString());
		} catch (NumberFormatException e) {
			logger.error("Invalid Port Input " + e.toString());
		} catch (Exception e) {
			logger.error("Connection error: " + e.toString());
		}
	}

	public void start() {

		try {
			// this.client.newConnection("localhost", 11002);
			System.out.println("Client Has Started. Enter 'Help' For More Information");
			Scanner s = new Scanner(System.in);
			s.useDelimiter("\n");
			while (this.running) {
				// String key = s.next();
				// String value = s.next();

				// if (value.equals("g")) {
				// this.client.get(key);
				// } else {
				// this.client.put(key, value);
				// }
				System.out.print(">>> ");
				parser(s.next());

			}
		} catch (Exception e) {
			logger.error("Connection error: " + e.toString());
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
