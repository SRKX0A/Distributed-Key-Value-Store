package app_kvServer;

import java.io.*;

import org.apache.log4j.Level;
import org.apache.commons.cli.*;

import logger.LogSetup;

public class App_KVServer {

    public static void main(String[] args) {

	Option bootstrapOption = Option.builder("b").desc("bootstrap server location as <address>:<port>").hasArg().required().type(String.class).build();
	Option portOption = Option.builder("p").desc("port which server listens on").hasArg().required().type(String.class).build();
	Option cacheSizeOption = Option.builder("c").desc("sets cache size on server").hasArg().type(String.class).build();
	Option strategyOption = Option.builder("s").desc("sets caching strategy on server").hasArg().type(String.class).build();
	Option addressOption = Option.builder("a").desc("address which server listens to").hasArg().type(String.class).build();
	Option directoryOption = Option.builder("d").desc("directory for persistent files").hasArg().type(String.class).build();
	Option logOption = Option.builder("l").desc("relative path of the logfile").hasArg().type(String.class).build();
	Option logLevelOption = Option.builder("ll").desc("loglevel, e.g. INFO, WARN, DEBUG, etc.").hasArg().type(String.class).build();
	Option replicationDelayOption = Option.builder("t").desc("replication delay in milliseconds").hasArg().type(String.class).build();
	Option helpOption = Option.builder("h").desc("displays help message").build();
	
	Options options = new Options();	
	options.addOption(bootstrapOption);
	options.addOption(portOption);
	options.addOption(cacheSizeOption);
	options.addOption(strategyOption);
	options.addOption(addressOption);
	options.addOption(directoryOption);
	options.addOption(logOption);
	options.addOption(logLevelOption);
	options.addOption(replicationDelayOption);
	options.addOption(helpOption);

	CommandLineParser parser = new DefaultParser();
	CommandLine cmd = null;
	try {
	    cmd = parser.parse(options, args);
	} catch (ParseException pe) {
	    System.err.println("Parsing failed. Reason: " + pe.getMessage());
	    System.exit(1);
	}

	String bootstrapAddress = null;
	int bootstrapPort = 0;
	int port = 0;
	int cacheSize = 100;
	String cacheStrategy = "None";
	String address = "localhost";
	String directory = "./";
	String logPath = "server.log";
	Level logLevel = Level.ALL;
	long replicationDelay = 500L;

	try {
	    String bootstrap = cmd.getOptionValue("b");  
	    bootstrapAddress = bootstrap.substring(0, bootstrap.indexOf(':'));
	    bootstrapPort = (int) Integer.parseInt(bootstrap.substring(bootstrap.indexOf(':') + 1));
	} catch (Exception e) {
	    System.err.println("Parsing failed. Reason: " + e.getMessage());
	    System.exit(1);
	}

	try {
	    port = (int) Integer.parseInt(cmd.getOptionValue("p"));
	} catch (Exception pe) {
	    System.err.println("Parsing failed. Reason: " + pe.getMessage());
	    System.exit(1);
	}

	if (cmd.hasOption("c")) {
	    try {
		cacheSize = (int) Integer.parseInt(cmd.getOptionValue("c"));
		if (cacheSize <= 0 || cacheSize >= 16777216) {
		    throw new IllegalArgumentException("Error: cache size must be between 1 and 16777215"); 
		}
	    } catch (Exception e) {
		System.err.println("Parsing failed. Reason: " + e.getMessage());
		System.exit(1);
	    }
	}

	if (cmd.hasOption("t")) {
	    try {
		replicationDelay = (long) Long.parseLong(cmd.getOptionValue("t"));
		if (replicationDelay <= 0) {
		    throw new IllegalArgumentException("Error: replication delay must be greater than 0"); 
		}
	    } catch (Exception e) {
		System.err.println("Parsing failed. Reasion: " + e.getMessage());
		System.exit(1);
	    }
	}

	if (cmd.hasOption("s")) {
	    cacheStrategy = cmd.getOptionValue("s");
	}
	
	if (cmd.hasOption("a")) {
	    address = cmd.getOptionValue("a"); 
	}

	if (cmd.hasOption("d")) {
	   directory = cmd.getOptionValue("d"); 
	}

	if (cmd.hasOption("l")) {
	    logPath = cmd.getOptionValue("l");
	}

	if (cmd.hasOption("ll")) {
	    logLevel = Level.toLevel(cmd.getOptionValue("ll"));
	}

        try {
            new LogSetup(logPath, logLevel);	

	    File store_dir = new File(directory);
	    store_dir.mkdirs();

	    File wal = new File(directory, "wal.txt");
	    wal.createNewFile();

	    KVServer kvServer = new KVServer(address, port, bootstrapAddress, bootstrapPort, directory, cacheSize, replicationDelay);
	    KVServerShutdownHook kvServerShutdownHook = new KVServerShutdownHook(kvServer);
	    Runtime.getRuntime().addShutdownHook(kvServerShutdownHook);
	    kvServer.start();
        } catch (IOException ioe) {
	    System.err.println("Could not set up server: " + ioe.getMessage());
            ioe.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.err.println("Error! Invalid argument <port>! Not a number!");
            System.exit(1);
        }

    }

}

class KVServerShutdownHook extends Thread {

    private KVServer kvServer;

    public KVServerShutdownHook(KVServer kvServer) {
	this.kvServer = kvServer;
    }

    @Override
    public void run() {
	this.kvServer.sendShutdownMessage();
	this.kvServer.close();
    }

}

