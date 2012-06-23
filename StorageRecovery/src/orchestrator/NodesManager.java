package orchestrator;

import java.util.Iterator;
import java.util.Set;

import orchestrator.OrchestratorDB.NodeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodesManager extends Thread {

	static final Logger logger = LoggerFactory.getLogger(NodesManager.class);
	OrchestratorDB orch_db;
	
	public NodesManager(OrchestratorDB orch_db){
		logger.info("Initalizing NodesManager");
		this.orch_db = orch_db;
	}
	
	public void run(){
		logger.info("Starting NodesManager");
		Set<String> ids = orch_db.nodes.keySet();
		
		while(true){
			Iterator<String> iterator = ids.iterator();
			while(iterator.hasNext()){
				String id = iterator.next();
				NodeInfo node_info = orch_db.nodes.get(id);
				boolean prev_status = node_info.isAlive();
				node_info.refreshStatus(orch_db.time_out);
				boolean current_status = node_info.isAlive();
				
				if(prev_status == false && current_status == true){
					//Dead -> Alive
					logger.info("Node with ID: {} is alive now", id);
					handleAliveNode(node_info);
				}else if(prev_status == true && current_status == false){
					//Alive -> Dead
					logger.info("Node with ID: {} is dead now", id);
					handleDeadNode(node_info);
				}
				
			}
			
			
			try {
				sleep(orch_db.refresh_rate);
			} catch (InterruptedException e) {
				logger.warn("NodesManager interrupted while sleeping!", e);
			}			
			
		}
	}
	
	
	private void handleAliveNode(NodeInfo node_info){
		
	}
	
	private void handleDeadNode(NodeInfo node_info){
		System.err.println("Node " + node_info.id + " Is dead!");
	}
}
