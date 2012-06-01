package orchestrator;

import shared.NodeAddress;

public class NodeInfo {
	
	public enum NodeType{
		PartitionManager,
		DataNode
	}
	
	private NodeAddress node_address;
	private long last_hearbrat;
	private NodeType node_type;

	private boolean is_alive;
	

}
