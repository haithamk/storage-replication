package shared;
import orchestrator.Orchestrator;
import partitionManager.PartitionManager;


public class StorageRecoveryMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String type = args[0];
		String node_id = args[1];
		String config_file = args[2];
		
		
		if(type.equals("Orchestrator")){
			Orchestrator orch = new Orchestrator(config_file, node_id);
			orch.run();
		}else if(type.equals("PartitionManager")){
			PartitionManager part_man = new PartitionManager(node_id, config_file);
			part_man.run();
		}else if(type.equals("DataNode")){
			
		}else{
			System.err.println("Unkown type!");
		}
		
		

	}

}
