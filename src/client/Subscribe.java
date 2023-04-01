package client;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;

import org.apache.log4j.Logger;

import shared.*;
import shared.messages.KVMessage;

public class Subscribe {
	
	private Socket socket;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private InputStream input;
	private OutputStream output;


	public Subscribe(Socket socket) throws IOException{
		this.socket = socket;
		this.input = socket.getInputStream();
		this.output = socket.getOutputStream();
	}
	
	public Socket getSocket(){
		return this.socket;
	}

}
