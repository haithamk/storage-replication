package orchestrator;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import messages.LogMessage;
import messages.LogResult;
import messages.RecoverDNMessage;
import messages.Message.MessageType;
import messages.RecoverPMMessage;
import orchestrator.OrchestratorDB.NodeInfo;
import orchestrator.OrchestratorDB.NodeInfo.NodeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utilities.NoCloseOutputStream;

public class NodesManager extends Thread {

	static final Logger logger = LoggerFactory.getLogger(NodesManager.class);
	OrchestratorDB orch_db;
	boolean new_pm_needed = false;
	
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
			
			if(new_pm_needed){
				electNewPM();
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
	
	private void handleDeadNode(NodeInfo dead_node_info){
		System.err.println("Node " + dead_node_info.id + " Is dead!");
		
		//TODO review synchronization
		if(dead_node_info.id.equals(orch_db.active_pm.id)){
			logger.info("Current Partition Manager({}) is dead. Electing new Partition Manager", dead_node_info.id);
			new_pm_needed = true;
		}else if(dead_node_info.type == NodeType.DataNode){
			handleDeadDNNode(dead_node_info);
		}
	}
	
	
	private void handleDeadDNNode(NodeInfo dead_node){
		orch_db.replicas_per_node.put(dead_node, 0);
		RecoverDNMessage recover_dn_message = new RecoverDNMessage(dead_node.address);
		
		Set<String> table_names = orch_db.tables_replicas.keySet();
		Iterator<String> name_itr = table_names.iterator();
		while(name_itr.hasNext()){
			String table_name = name_itr.next();
			NodeInfo[] nodes = orch_db.tables_replicas.get(table_name);
			for(int i = 0; i < nodes.length; i++){
				NodeInfo node = nodes[i];
				if(node == dead_node){
					NodeInfo new_node = assignNewReplica(table_name, nodes);
					recover_dn_message.table_names.add(table_name);
					recover_dn_message.table_names.add(new_node.address);
					for(int j = 0; j < nodes.length; j++){
						if(nodes[j] == dead_node){
							nodes[j] = new_node;
						}
					}
					break;
				}
			}
		}
		
		sendRecoverDNMessage(recover_dn_message);
	}
	
	
	private void sendRecoverDNMessage(RecoverDNMessage recover_dn_message){
		if(recover_dn_message.table_names.size() == 0 || orch_db.active_pm == null){
			return;
		}
		
		Socket socket = null;
		PrintWriter out = null;
        try {   
        	
        	//Create socket to the DN
        	String address =  orch_db.active_pm.address;
        	String ip = address.split(":")[0];
        	int port = Integer.parseInt(address.split(":")[1]);
            socket = new Socket(ip, port);
            
            //Init output stream
            out = new PrintWriter(new NoCloseOutputStream(socket.getOutputStream()), true);
            XMLStreamWriter xsw = XMLOutputFactory.newInstance().createXMLStreamWriter(socket.getOutputStream()); 
            
            //Send operation type
            out.print(MessageType.RECOVER_DN_MESSAGE + "\n");
            out.flush();
            
            //Send log message
            JAXBContext jaxb_context = JAXBContext.newInstance(RecoverDNMessage.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.setProperty(Marshaller.JAXB_FRAGMENT,true);
			m.marshal(recover_dn_message,xsw);
	        xsw.flush();    // send it now

	        //Close connection
            xsw.close();
            out.close();
            socket.close();
        } catch (JAXBException e) {
			logger.error("Error marshling/unmarshling", e);
		} catch (UnknownHostException e) {
			logger.error("Error communicating with the remote node", e);
		} catch (IOException e) {
			logger.error("Error communicating with the remote node", e);
		} catch (XMLStreamException e) {
			logger.error("Error in the XML reader/writer", e);
		} catch (FactoryConfigurationError e) {
			logger.error("Error in the XML reader/writer", e);
		}
	}
	
	private NodeInfo assignNewReplica(String table_name, NodeInfo[] assigned_nodes){
		NodeInfo new_node = null;
		int min_tables = 0;
		Set<NodeInfo> nodes = orch_db.replicas_per_node.keySet();
		Iterator<NodeInfo> node_itr = nodes.iterator();
		while(node_itr.hasNext()){
			NodeInfo node = node_itr.next();
			boolean already_assigned = false;
			for(int i = 0; i < assigned_nodes.length; i++){
				if(assigned_nodes[i] == node){
					already_assigned = true;
					break;
				}
			}
			
			int tables_num = orch_db.replicas_per_node.get(node);
			if(tables_num < min_tables && !already_assigned){
				new_node = node;
				min_tables = tables_num;
			}
			
		}
		
		if(new_node != null){
			orch_db.replicas_per_node.put(new_node, ++min_tables);
		}
		
		return new_node;
	}
	
	private void electNewPM(){
		NodeInfo new_active_pm = null;
		Set<String> ids = orch_db.nodes.keySet();
		Iterator<String> iterator = ids.iterator();
		while(iterator.hasNext()){
			String id = iterator.next();
			NodeInfo node_info = orch_db.nodes.get(id);
			if(node_info.type == NodeType.PartitionManager && node_info.isAlive()){
				new_active_pm = node_info;
				break;
			}
		}
		
		if(new_active_pm != null){
			logger.info("New active Partiton Mananger id: {}", new_active_pm.id);
			new_pm_needed = false;
			handleNewPM(new_active_pm);
		}else{
			logger.warn("No availbe Partition Managers");
		}
	}

	
	private void handleNewPM(NodeInfo new_pm){
		RecoverPMMessage recover_pm = new RecoverPMMessage();
		
		Set<String> table_names = orch_db.tables_replicas.keySet();
		Iterator<String> table_itr = table_names.iterator();
		while(table_itr.hasNext()){
			String table_name = table_itr.next();
			NodeInfo[] nodes = orch_db.tables_replicas.get(table_name);
			recover_pm.table_names.add(table_name);
			String[] replicas = new String[nodes.length]; 
			for(int i = 0; i < nodes.length; i++){
				replicas[i] = nodes[i].address;
			}
			recover_pm.replicas.add(replicas);
		}
		
		//TODO send the message to the PM
		
		orch_db.setActivePM(new_pm);
	}
}
