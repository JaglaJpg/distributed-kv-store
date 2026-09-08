package kv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class KVClient {
	private static final String DEFAULT_SERVER_NAME = "localhost";
	private static final int DEFAULT_SERVER_PORT = 4545; 
	
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		BufferedReader in = null;
		PrintWriter out = null;
		Socket kvClientSocket = null;
		
		try {
			kvClientSocket = new Socket(DEFAULT_SERVER_NAME, DEFAULT_SERVER_PORT);
			out = new PrintWriter(kvClientSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(kvClientSocket.getInputStream()));
		} catch (UnknownHostException e) {
            System.err.println("Don't know about host: localhost ");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to: "+ DEFAULT_SERVER_PORT);
            System.exit(1);
        }
		
		BufferedReader userIn = new BufferedReader(new InputStreamReader(System.in));
		String fromServer;
		String fromUser;
		
		
		while (true) {
			fromUser = userIn.readLine();
			if (fromUser != null) {
				System.out.println("Client: " + fromUser);
				out.println(fromUser);
			}
			
			fromServer = in.readLine();
        	System.out.println("Server: " + fromServer);
			
		}
		
	}
}
