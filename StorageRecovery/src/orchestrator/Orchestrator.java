package orchestrator;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Orchestrator {
	
	

	
	private ExecutorService pool;
	private ServerSocket commSocket;
	OrchestratorDB orch_db;
	
	
	/**
	 * Initialize the Orchestrator
	 * @param config_file the configuration file to initialize the Orchestrator parameters
	 */
	public Orchestrator(String config_file, String id){
		try
		{
			orch_db = new OrchestratorDB(config_file, id);
			pool = Executors.newCachedThreadPool();
			// TODO fixed size or dynamic ?
			//pool = Executors.newFixedThreadPool(Integer.parseInt(pool_size)); 
			commSocket = new ServerSocket(orch_db.port);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * Starts the Orchestrator. 
	 */
	public void run(){
		
		Socket socket = null;
		
		while(true){
			try {
				socket = commSocket.accept();
				//Execute method is NOT blocking function. The Job is saved and when
				//There are available thread it will handle it.
				pool.execute(new OrchMessageHandler(socket, orch_db));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
}
