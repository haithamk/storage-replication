import orchestrator.Orchestrator;


public class main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String type = args[0];
		String ip = args[1];
		String port = args[2];
		
		
		if(type.equals("Orchestrator")){
			Orchestrator orch = new Orchestrator(config_file);
			
		}else if(type.equals("PartitionManager")){
			
		}else if(type.equals("DataNode")){
			
		}else{
			System.err.println("Unkown type!");
		}
		
		

	}

}
