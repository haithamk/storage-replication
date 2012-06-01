package orchestrator;

import java.util.LinkedList;

import shared.NodeAddress;
import shared.TableInfo;

public class Orchestrator {
	
	
	//Used to keep track of the nodes in the system
	private LinkedList<NodeInfo> nodes_details;
	//The current active partition manager
	private NodeInfo current_pm;
	
	private LinkedList<TableInfo> tables_list;
	
	
	
	
	public Orchestrator(String config_file){
		//TODO init DSs
		//TODO read configuration
	}
	
	
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
