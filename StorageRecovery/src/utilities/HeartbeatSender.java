package utilities;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatSender extends Thread {

	static final Logger logger = LoggerFactory.getLogger(HeartbeatSender.class);
	
	DatagramPacket packet;
	DatagramSocket client_socket;
	
	public HeartbeatSender(String id, int rate, String dest_ip, int dest_port){
		try{
			byte[] id_bytes = id.getBytes();			
			InetAddress dest_address = InetAddress.getByName(dest_ip);
			
			this.packet = new DatagramPacket(id_bytes, id_bytes.length, dest_address, dest_port);
			this.client_socket = new DatagramSocket();
			
		}catch(UnknownHostException e){
			logger.error("Error initalizing the dest address: Unkwon host", e);
		} catch (SocketException e) {
			logger.error("Error while creating a DatagramSocket", e);
		}
		
	}
	
	
	@Override
	public void run(){
		
		logger.info("Heartbeat sender started");
		if(packet == null || client_socket == null){
			logger.error("Trying to start HeartbeatSender with unvalid parameters. exiting..");
			return;
		}
		
		
		while(true){
			
			try {
				client_socket.send(packet);
			} catch (IOException e) {
				logger.warn("Error while sending heartbeat packet to the destination", e);
			}
			
		}
	}
}
