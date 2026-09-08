package KV;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class KVClient {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException{
		String serverName = "localhost";
		int serverPort = 4545; 
		BufferedReader in = null;
		PrintWriter out = null;
		Socket KVClientSocket = null;
		
		try {
			KVClientSocket = new Socket(serverName, serverPort);
			out = new PrintWriter(KVClientSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(KVClientSocket.getInputStream()));
		} catch (UnknownHostException e) {
            System.err.println("Don't know about host: localhost ");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to: "+ serverPort);
            System.exit(1);
        }
		
		BufferedReader userIn = new BufferedReader(new InputStreamReader(System.in));
		String fromServer;
		String fromUser;
		
		
		while(true) {
			fromUser = userIn.readLine();
			if(fromUser != null) {
				System.out.println("Client: " + fromUser);
				out.println(fromUser);
			}
			
			fromServer = in.readLine();
        	System.out.println("Server: " + fromServer);
			
		}
		
	}
}
