package app_kvServer;

public class Connection extends Thread {
    
    private static Logger logger = Logger.getRootLogger();

    private Socket socket;
    private boolean isOpen;

    private InputStream input;
    private OutputStream output;

    public Connection(Socket socket) {
	this.socket = socket;
	this.isOpen = true;

    }

    public void run() {
	
	try {

	    this.input = socket.getInputStream();
	    this.output = socket.getOutputStream();

	    

	} catch(IOException e) {
	    logger.error(e.toString());
	}

    }

    public void sendMessage(ProtocolMessage pm) throws IOException {

    }
}
