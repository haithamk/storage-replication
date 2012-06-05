package orchestrator;

import java.io.ObjectInputStream;
import java.net.Socket;

public class MessageHandler implements Runnable {
	
	Socket socket;
	public MessageHandler(Socket socket ){
		this.socket = socket;
	}

	@Override
	public void run() {
		try{
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			//TODO read the message from the socket. Unmrashall it
			//execute it and return the result to the client.
		}catch(Exception e){
			e.printStackTrace();
		}
		

	}

}
