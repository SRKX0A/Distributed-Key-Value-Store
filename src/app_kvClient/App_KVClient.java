package app_kvClient;

import java.io.*;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.cli.*;

import logger.LogSetup;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class App_KVClient extends Thread {

	public static Logger logger = Logger.getRootLogger();

	private KVClient client;
	private ClientSubscriptionServer subscriptionServer;

	boolean running;
	boolean connected;

	public App_KVClient(String address, int port) throws IOException {
		this.running = true;
		this.connected = false;

		this.client = new KVClient();

		this.subscriptionServer = new ClientSubscriptionServer(address, port);	
		this.subscriptionServer.start();
	}

	public void parser(String message) throws Exception {
		String[] commands = message.split(" ");

		switch (commands[0]) {
			case "quit":
				shutClientDown(commands);
				break;
			case "connect":
				connectToServer(commands);
				break;
			case "disconnect":
				disconnectFromServer(commands);
				break;
			case "put":
				updateValue(commands);
				break;
			case "get":
				getValue(commands);
				break;
			case "keyrange":
				keyrangeMessage(commands);
				break;
			case "keyrange_read":
				keyrangeReadMessage(commands);
				break;
			case "subscribe":
				subscribeMessage(commands);
				break;
			case "unsubscribe":
				unsubscribeMessage(commands);
				break;
			case "help":
				helpMessage();
				break;
			case "logLevel":
				setLogLevel(commands);
				break;
			default:
				System.out.println("Unknown command");
				helpMessage();

		}
	}

	public void setLogLevel(String[] commands) throws Exception {
		if (commands.length != 2) {
			System.out.println("Invalid Number of Arguments");
			return;
		}

		switch (commands[1]) {
			case "ALL":
				this.client.logLevel(Level.ALL);
				System.out.println("Logger Level Set To: " + this.client.getLogLevel());
				break;
			case "DEBUG":
				this.client.logLevel(Level.DEBUG);
				System.out.println("Logger Level Set To: " + this.client.getLogLevel());
				break;
			case "INFO":
				this.client.logLevel(Level.INFO);
				System.out.println("Logger Level Set To: " + this.client.getLogLevel());
				break;
			case "WARN":
				this.client.logLevel(Level.WARN);
				System.out.println("Logger Level Set To: " + this.client.getLogLevel());
				break;
			case "ERROR":
				this.client.logLevel(Level.ERROR);
				System.out.println("Logger Level Set To: " + this.client.getLogLevel());
				break;
			case "FATAL":
				this.client.logLevel(Level.FATAL);
				System.out.println("Logger Level Set To: " + this.client.getLogLevel());
				break;
			case "OFF":
				this.client.logLevel(Level.OFF);
				System.out.println("Logger Level Set To: " + this.client.getLogLevel());
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
		System.out.println("To Get keyrange message -> keyrange");
		System.out.println("To Get keyrange_read message -> keyrange_read");
		System.out.println("To Subscribe -> subscribe <key>");
		System.out.println("To Unsubscribe -> unsubscribe <key>");
		System.out.println("To Change LogLevel -> logLevel <level>");
		System.out.println("To Quit -> quit");
		System.out.println("For Help -> help");
	}

	public void getValue(String[] commands) {
		try {
			if (!this.connected) {
				System.out.println("Client currently not connected to a server. Please Connect To A Server");
				return;
			}
			if (commands.length != 2) {
				System.out.println("Invalid Number of Arguments");
				return;
			}
			KVMessage message = this.client.get(commands[1]);
			System.out.println((message.getValue() == null || message.getValue().equals("null")) ? "Error: Key Not Found" : message.getValue());

		} catch (EOFException e) {
		    logger.info("Error: Server disconnected: " + e.toString());	
		    System.out.println("Error: Server disconnected: " + e.toString());	
		    this.connected = false;
		    this.client.disconnect();
		} catch (Exception e) {
			logger.error("ERROR: " + e.toString());
			System.out.println("ERROR: " + e.toString());
		}
	}

	public void updateValue(String[] commands) {
		try {
			if (!this.connected) {
				System.out.println("Client currently not connected to a server. Please Connect To A Server");
				return;
			}
			if (commands.length != 3) {
				System.out.println("Invalid Number of Arguments");
				return;
			}

			String[] value = new String[commands.length - 2];
			System.arraycopy(commands, 2, value, 0, commands.length - 2); 
			String result = String.join(" ", value);
			KVMessage message = this.client.put(commands[1], result);
			if (message.getStatus() == StatusType.PUT_SUCCESS || message.getStatus() == StatusType.PUT_UPDATE) {
			    System.out.println("SUCCESS");
			} else {
			    System.out.println("ERROR");
			}

		} catch (EOFException e) {
		    logger.info("Error: Server disconnected: " + e.toString());	
		    System.out.println("Error: Server disconnected: " + e.toString());	
		    this.connected = false;
		    this.client.disconnect();
		} catch (Exception e) {
			logger.error("ERROR: " + e.toString());
			System.out.println("ERROR: " + e.toString());
		}
	}

    public void keyrangeMessage(String[] commands) {

	if (!this.connected) {
	    System.out.println("Client currently not connected to a server. Please Connect To A Server");
	    return;
	}

	if (commands.length != 1) {
	    System.out.println("Invalid Number of Arguments");
	    return;
	}

	try {
	    KVMessage message = this.client.keyrange();
	    System.out.println(message.getKey());
	} catch (EOFException e) {
	    logger.info("Error: Server disconnected: " + e.toString());	
	    System.out.println("Error: Server disconnected: " + e.toString());	
	    this.connected = false;
	    this.client.disconnect();
	} catch (Exception e) {
	    logger.error("ERROR: " + e.toString());
	    System.out.println("ERROR: " + e.toString());
	}

    }

    public void keyrangeReadMessage(String[] commands) {

	if (!this.connected) {
	    System.out.println("Client currently not connected to a server. Please Connect To A Server");
	    return;
	}

	if (commands.length != 1) {
	    System.out.println("Invalid Number of Arguments");
	    return;
	}

	try {
	    KVMessage message = this.client.keyrangeread();
	    System.out.println(message.getKey());
	} catch (EOFException e) {
	    logger.info("Error: Server disconnected: " + e.toString());	
	    System.out.println("Error: Server disconnected: " + e.toString());	
	    this.connected = false;
	    this.client.disconnect();
	} catch (Exception e) {
	    logger.error("ERROR: " + e.toString());
	    System.out.println("ERROR: " + e.toString());
	}

    }
    
    
    public void subscribeMessage(String[] commands) {
    
	if (commands.length != 2) {
	    System.out.println("Invalid Number of Arguments");
	    return;
	}

	try {
	    KVMessage reply = this.client.subscribe(commands[1], this.subscriptionServer.getAddress(), this.subscriptionServer.getPort());
	    System.out.println(reply.getStatus());
	} catch(Exception e) {
	    logger.error("ERROR: " + e.getMessage());
	    System.out.println("ERROR: " + e.getMessage());
	}
    
    }
    
    public void unsubscribeMessage(String[] commands){
    	
	if (commands.length != 2) {
	  System.out.println("Invalid Number of Arguments");
	  return;
	}

	try {
	    KVMessage reply = this.client.unsubscribe(commands[1], this.subscriptionServer.getAddress(), this.subscriptionServer.getPort());
	    System.out.println(reply.getStatus());
	} catch(Exception e) {
	    logger.error("ERROR: " + e.getMessage());
	    System.out.println("ERROR: " + e.getMessage());
	}
    
    }

	public void shutClientDown(String[] commands) throws Exception {
		if (commands.length != 1) {
		    System.out.println("Invalid Number of Arguments");
		    return;
		}
		if (this.connected) {
			disconnectFromServer(commands);
		}
		this.subscriptionServer.close();
		System.out.println("Client Shutting Down...");
		TimeUnit.SECONDS.sleep(1);
		System.exit(1);
	}

	public void disconnectFromServer(String[] commands) {
		if (commands.length != 1) {
		    System.out.println("Invalid Number of Arguments");
		    return;
		}
		if (!this.connected) {
			System.out.println("Client currently not connected to a server");
			return;
		}
		try {
			this.connected = false;
			this.client.disconnect();
			logger.info("Client Disconnected From Server");
			System.out.println("Client Disconnected From Server");
		} catch (Exception e) {
			logger.error("Disconnection Error: " + e.toString());
			System.out.println("Disconnection Error: " + e.toString());
		}
	}

	public void connectToServer(String[] commands) {
		try {
			if (commands.length != 3) {
				System.out.println(
						"Invalid Number of Arguments");
				return;
			}
			if (this.connected) {
				System.out.println(
						"Client already Connected to Server. Please Disconnect First to Connect to another Server");
				return;
			}
			String serverAddress = commands[1];
			Integer serverPort = Integer.parseInt(commands[2]);
			this.client.newConnection(serverAddress, serverPort);
			String connectMessage = MessageFormat.format("Connection established with server at {0}:{1}", serverAddress,
					serverPort.toString());
			this.connected = true;
			logger.info(connectMessage);
			System.out.println(connectMessage);

		} catch (UnknownHostException e) {
			logger.error("Unknown Host: " + e.toString());
			System.out.println("Unknown Host: " + e.toString());
		} catch (NumberFormatException e) {
			logger.error("Invalid Port Input: " + e.toString());
			System.out.println("Invalid Port Input: " + e.toString());
		} catch (Exception e) {
			logger.error("Connection error: " + e.toString());
			System.out.println("Connection error: " + e.toString());
		}
	}

	public void start() {

		try {
			System.out.println("Client Has Started. Enter 'Help' For More Information");
			Scanner s = new Scanner(System.in);
			s.useDelimiter("\n");
			while (this.running) {
				System.out.print(">>> ");
				parser(s.next());

			}
		} catch (Exception e) {
			logger.error("Connection error: " + e.toString());
			System.out.println("Connection error: " + e.toString());
		}

	}

	public static void main(String[] args) {

	    Option portOption = Option.builder("p").desc("port which client subscription server listens on").hasArg().required().type(String.class).build();
	    Option addressOption = Option.builder("a").desc("address which client subscription server listens to").hasArg().type(String.class).build();

	    Options options = new Options();	
	    options.addOption(portOption);
	    options.addOption(addressOption);

	    CommandLineParser parser = new DefaultParser();
	    CommandLine cmd = null;

	    try {
		cmd = parser.parse(options, args);
	    } catch (ParseException pe) {
		System.err.println("Parsing failed. Reason: " + pe.getMessage());
		System.exit(1);
	    }

	    int port = 0;
	    String address = "localhost";

	    try {
		port = (int) Integer.parseInt(cmd.getOptionValue("p"));
	    } catch (Exception pe) {
		System.err.println("Parsing failed. Reason: " + pe.getMessage());
		System.exit(1);
	    }

	    if (cmd.hasOption("a")) {
		address = cmd.getOptionValue("a"); 
	    }

	    try {
		new LogSetup("logs/client.log", Level.OFF);
		new App_KVClient(address, port).start();
	    } catch (IOException e) {
		System.out.println("Error! Unable to initialize logger!");
		e.printStackTrace();
		System.exit(1);
	    }
    }

}
