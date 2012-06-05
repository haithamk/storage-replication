package orchestrator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import shared.NodeAddress;
import shared.TableInfo;

public class Orchestrator {
	
	

	
	private ExecutorService pool;
	private ServerSocket commSocket;
	
	
	/**
	 * Initialize the Orchestrator
	 * @param config_file the configuration file to initialize the Orchestrator parameters
	 */
	public Orchestrator(String config_file){
		//TODO read configuration		
		
		
		String port = "";
		String pool_size = "";
		
		try
		{
			pool = Executors.newCachedThreadPool();
			// TODO fixed size or dynamic ?
			//pool = Executors.newFixedThreadPool(Integer.parseInt(pool_size)); 
			commSocket = new ServerSocket(Integer.parseInt(port));
		}
		catch(IOException e)
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
				pool.execute(new MessageHandler(socket));
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
		}
	}
	
	
	
	public String getActivePMAddress(){
		return "";		
	}
	
	
	
	//***************************************************************************
	//***************************************************************************
	//***************************************************************************
	//***************************************************************************
	
	// OLD METHODS, IGNORE
	
	
	
	//Used to keep track of the nodes in the system
	private LinkedList<NodeInfo> nodes_details;
	//The current active partition manager
	private NodeInfo current_pm;
	
	private LinkedList<TableInfo> tables_list;
	
	
	
	public void recieveHeartBeat(NodeAddress from){
		
	}
	
	
	
	
	
	public void handleDeadDataNode(NodeAddress from){
		
	}
	
	
	
	
	public void handleDeadPartitionManager(NodeAddress from){
		
	}
	
	
	
	public TableInfo chooseTableNodes(String user_name, String table_name){
		return null;
	}
	
	
	
}
