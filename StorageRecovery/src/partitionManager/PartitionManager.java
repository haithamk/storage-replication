package partitionManager;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utilities.HeartbeatSender;


public class PartitionManager {
	
	static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);
	PartitionManagerDB pm_db;
	private ExecutorService pool;
	private ServerSocket commSocket;
	private HeartbeatSender heartbeat_sender;
	
	
	public PartitionManager(String node_id, String config_file){
		try{
			logger.info("Initalizing PartitionManager({})", node_id);			
			pm_db = new PartitionManagerDB(node_id, config_file);
			pool = Executors.newCachedThreadPool();
			commSocket = new ServerSocket(pm_db.port);
			//heartbeat_sender = new HeartbeatSender(node_id, pm_db.heartbeat_rate, pm_db.orch_ip, pm_db.orch_port);
			logger.info("PartitionManager({}) initalized successfully", node_id);
		}catch(Exception e){
			logger.error("An error occurred while initalizing PartitionManager(" + node_id + ")", e);
		}
	}
	
	
	
	public void run(){
		
		//heartbeat_sender.start();
		
		Socket socket = null;		
		logger.info("Starting the PartitionManager({})", pm_db.node_id);
		while(true){
			try {
				socket = commSocket.accept();
				logger.debug("New socket recieved");
				//Execute method is NOT blocking function. The Job is saved and when
				//There are available thread it will handle it.
				pool.execute(new PMMessageHandler(socket, pm_db));
			} catch (IOException e) {
				logger.error("An error occurred while running PartitionManager(" + pm_db.node_id + ")", e);
			}
		}
	}
	
	

}
