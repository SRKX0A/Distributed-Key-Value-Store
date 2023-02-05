package testing;

import java.io.*;
import java.net.*;
import java.util.Arrays;

import app_kvServer.KVServer;
import app_kvClient.KVClient;
import client.KVStore;
import client.ProtocolMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import org.junit.Test;

import junit.framework.TestCase;


public class ProtocolTest extends TestCase {

    public void setUp() {

    }

    public void tearDown() {

    }


    @Test
    public void testProtocolEncode() {

	Exception ex = null;

	byte[] msgBytesTarget = {'P', 'U', 'T', ' ', 'k', 'e', 'y', ' ', 'v', 'a', 'l', 'u', 'e', '\r', '\n'};
	byte[] msgBytes = null;

	ProtocolMessage msg = new ProtocolMessage(StatusType.PUT, "key", "value");
	try {
	    msgBytes = (msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue() + "\r\n").getBytes("UTF-8");
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && Arrays.equals(msgBytes, msgBytesTarget));	

    }

    @Test
    public void testProtocolDecodeAtServer() {

	Exception ex = null;

	byte[] msgBytes = {'P', 'U', 'T', ' ', 'k', 'e', 'y', ' ', 'v', 'a', 'l', 'u', 'e', '\r', '\n'};
	ProtocolMessage msg = null;

	try {
	    msg = ProtocolMessage.fromBytesAtServer(msgBytes);
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && msg.getStatus() == StatusType.PUT && msg.getKey().equals("key") && msg.getValue().equals("value"));	

    }

}
