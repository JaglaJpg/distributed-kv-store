package KV;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVServer {
	
	public static void main(String[] args) throws IOException {
		
		ServerSocket KVServerSocket = null;
		boolean listening = true;
		String kvServerName = "KVServer";
		int KVServerNo = 4545;
		
		Map<String, Entry> store = new ConcurrentHashMap<String, Entry>();
		
		KVSharedState ourSharedState = new KVSharedState(store);
		
		try {
			KVServerSocket = new ServerSocket(KVServerNo);
		} catch (IOException e) {
			System.err.println("Could not start " + kvServerName + " specified port.");
		      System.exit(-1);
		}
		
		System.out.println(kvServerName + " started");
		
		while(listening) {
			new KVThread(KVServerSocket.accept(), ourSharedState).start();
			System.out.println("New " + kvServerName + " thread started");
		}
		KVServerSocket.close();
	}

}
