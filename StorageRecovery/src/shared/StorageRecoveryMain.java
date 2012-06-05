package shared;
import orchestrator.Orchestrator;
import partitionManager.PartitionManager;


public class StorageRecoveryMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String type = args[0];
		String config_file = args[1];
		
		
		if(type.equals("Orchestrator")){
			Orchestrator orch = new Orchestrator(config_file);
			orch.run();
		}else if(type.equals("PartitionManager")){
			PartitionManager part_man = new PartitionManager(config_file);
			part_man.run();
		}else if(type.equals("DataNode")){
			
		}else{
			System.err.println("Unkown type!");
		}
		
		

	}

}
