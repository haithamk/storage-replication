package utilities;

import orchestrator.Orchestrator;
import partitionManager.PartitionManager;

import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.classic.LoggerContext;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class StorageRecoveryMain {

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {	
		String type = args[0];
		String node_id = args[1];
		String config_file = args[2];
		
		initLogger(node_id);		
		final Logger logger = LoggerFactory.getLogger(StorageRecoveryMain.class);
			
		logger.info("New node initiated with parameters: Type: " + type + " ID: " + node_id + " Config: " + config_file);
		
		
		if(type.equals("Orchestrator")){
			Orchestrator orch = new Orchestrator(node_id, config_file);
			orch.run();
		}else if(type.equals("PartitionManager")){
			PartitionManager part_man = new PartitionManager(node_id, config_file);
			part_man.run();
		}else if(type.equals("DataNode")){
			
		}else{
			logger.error("Unkown type!");
		}
		
		

	}

	private static void initLogger(String node_id) {
		try {
			LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
			JoranConfigurator jc = new JoranConfigurator();
			jc.setContext(context);
			context.reset(); // override default configuration
			
			// inject the id of the current node and the time stamp of the log file
			// property of the LoggerContext
			context.putProperty("node-id", node_id);
			context.putProperty("time", Long.toString(System.currentTimeMillis()) );			
			jc.doConfigure("logback.xml");
		} catch (JoranException e) {
			System.err.println("Error configuring Logger!");
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
}
