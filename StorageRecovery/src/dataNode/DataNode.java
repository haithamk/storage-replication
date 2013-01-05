package dataNode;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utilities.HeartbeatSender;


public class DataNode {
	
	static final Logger logger = LoggerFactory.getLogger(DataNode.class);
	private HeartbeatSender heartbeat_sender;
	private ServerSocket comm_socket;
	private DataNodeDB dn_db;
	
	String node_id = "";
	
	public DataNode(String node_id, String config_file){
		try {
			logger.info("Initalizing DataNode({})", node_id);
			//TODO init
			dn_db = new DataNodeDB(node_id, config_file);
			heartbeat_sender = new HeartbeatSender(node_id, dn_db.heartbeat_rate, dn_db.orch_ip, dn_db.orch_port);
			comm_socket = new ServerSocket(dn_db.port);
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
