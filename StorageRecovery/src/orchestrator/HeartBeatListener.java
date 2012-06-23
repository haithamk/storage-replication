package orchestrator;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utilities.HeartbeatSender;

public class HeartBeatListener extends Thread {

	static final Logger logger = LoggerFactory.getLogger(HeartBeatListener.class);
	OrchestratorDB orch_db;
	DatagramSocket serverSocket;
	
	public HeartBeatListener(OrchestratorDB orch_db) throws SocketException{
		logger.info("Initalizing HeartBeatListener");
		this.orch_db = orch_db;
		serverSocket = new DatagramSocket(orch_db.port);
		logger.info("HeartBeatListener initalized successfully");
	}
	
	
	@Override
	public void run(){
		
		byte[] received_buffer = new byte[1024];
		while(true){
			try {
				DatagramPacket received_packet = new DatagramPacket(received_buffer, received_buffer.length);
				serverSocket.receive(received_packet);
				String id = new String( received_packet.getData(), 0, received_packet.getLength());
				orch_db.logHeartbeat(id);
			} catch (IOException e) {
				logger.warn("Error while recieving UDP packet", e);
			}
			
		}
	}
}
