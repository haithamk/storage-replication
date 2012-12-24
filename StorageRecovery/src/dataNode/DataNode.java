package dataNode;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitionManager.PMMessageHandler;

import utilities.HeartbeatSender;


public class DataNode {
	
	static final Logger logger = LoggerFactory.getLogger(DataNode.class);
	private HeartbeatSender heartbeat_sender;
	private ServerSocket comm_socket;
	
	String node_id = "";
	
	public DataNode(String node_id, String config_file){
		try {
			logger.info("Initalizing DataNode({})", node_id);
			//TODO init
			heartbeat_sender = new HeartbeatSender(node_id, -1, "", -1);
			comm_socket = new ServerSocket(-1);
			logger.info("DataNode({}) initalized successfully", node_id);
		} catch (Exception e) {
			logger.error("An error occurred while initalizing DataNode(" + node_id + ")", e);
		}
		
	}
	
	
	public void run(){
		heartbeat_sender.start();
		
		Socket socket = null;		
		logger.info("Starting the DataNode({})", node_id);
		while(true){
			try {
				socket = comm_socket.accept();
				logger.debug("New socket recieved");
				//Execute method is NOT blocking function. The Job is saved and when
				//There are available thread it will handle it.
			} catch (IOException e) {
				logger.error("An error occurred while running PartitionManager(" + node_id + ")", e);
			}
		}
		
	}
}
