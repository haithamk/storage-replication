package orchestrator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

import shared.NodeAddress;
import shared.TableInfo;

public class Orchestrator {
	
	
	//Used to keep track of the nodes in the system
	private LinkedList<NodeInfo> nodes_details;
	//The current active partition manager
	private NodeInfo current_pm;
	
	private LinkedList<TableInfo> tables_list;
	
	
	private ServerSocket commSocket;
	
	
	/**
	 * Initialize the Orchestrator
	 * @param config_file the configuration file to initialize the Orchestrator parameters
	 */
	public Orchestrator(String config_file){
		//TODO read configuration		
		
		
		String port = "";
		
		try
		{
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
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
		}
	}
	
	
	
	
	
	//***************************************************************************
	//***************************************************************************
	//***************************************************************************
	//***************************************************************************
	
	
	public String getActivePMAddress(){
		return "";		
	}
	
	
	
	
	
	
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
