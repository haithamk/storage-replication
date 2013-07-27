package orchestrator;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Orchestrator {
	
	static final Logger logger = LoggerFactory.getLogger(Orchestrator.class);
	
	private ExecutorService pool;
	private ServerSocket commSocket;
	OrchestratorDB orch_db;
	HeartBeatListener heartbeat_listener;
	NodesManager nodes_manager;
	
	
	/**
	 * Initialize the Orchestrator
	 * @param config_file the configuration file to initialize the Orchestrator parameters
	 */
	public Orchestrator(String id, String config_file){
		try
		{
			logger.info("Initalizing Orchestrator");
			orch_db = new OrchestratorDB(id, config_file);
			pool = Executors.newCachedThreadPool();
			commSocket = new ServerSocket(orch_db.port);
			heartbeat_listener = new HeartBeatListener(orch_db);
			nodes_manager = new NodesManager(orch_db);
			logger.info("Orchestrator initalized successfully");
		}
		catch(Exception e)
		{
			logger.error("An error occurred while initalizing Orchestrator", e);
		}
	}
	
	
	
	/**
	 * Starts the Orchestrator. 
	 */
	public void run(){
		
		logger.info("Starting the Orchestrator");
		heartbeat_listener.start();
		nodes_manager.start();
		Socket socket = null;		
		while(true){
			try {
				socket = commSocket.accept();
				logger.debug("New socket recieved");
				//Execute method is NOT blocking function. The Job is saved and when
				//There are available thread it will handle it.
				pool.execute(new OrchMessageHandler(socket, orch_db));
			} catch (IOException e) {
				logger.error("An error occurred while running Orchestrator", e);
			}
		}
	}
	
	
	
}
