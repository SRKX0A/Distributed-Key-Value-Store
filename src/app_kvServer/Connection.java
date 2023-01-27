package app_kvServer;

public class Connection extends Thread {
    
    private static Logger logger = Logger.getRootLogger();
    
    private KVServer kvServer;

    private Socket socket;

    private InputStream input;
    private OutputStream output;

    private boolean isOpen;

    public Connection(Socket socket, KVServer kvServer) throws IOException {
	this.kvServer = kvServer;

	this.socket = socket;
	this.input = socket.getInputStream();
	this.output = socket.getOutputStream();

	this.isOpen = true;
    }

    public void run() {

	while (this.isOpen) {

	    try {

		ByteArrayInputStream bis = new ByteArrayInputStream(this.input);
		ObjectInputStream ois = new ObjectInputStream(bis);

		ProtocolMessage request = ois.readObject();
		ois.skipBytes(2);

		// TODO: handle incoming pm

		//placeholder reply message

		ProtocolMessage reply = new ProtocolMessage(KVMessage.StatusType.PUT_SUCCESS, null, null);

		ByteArrayOutputStream bos = new ByteArrayOutputStream(this.output);
		ObjectOutputStream oos = new ObjectOutputStream(bos);

		oos.writeObject(reply);
		oos.write('\r');
		oos.write('\n');
		oos.flush();
		oos.close();

	    } catch(IOException e) {
		logger.error(e.toString());
	    }

	}

    }
}
