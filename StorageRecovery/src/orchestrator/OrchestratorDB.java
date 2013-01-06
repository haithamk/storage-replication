package orchestrator;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import orchestrator.OrchestratorDB.NodeInfo.NodeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class OrchestratorDB {
	
	
	//=========================================================================
	//==================		Auxiliary classes				===============
	//=========================================================================
	
	
	static class NodeInfo{
		enum NodeType{
			PartitionManager,
			DataNode
		}
		
		String id;
		NodeType type;
		String address;
		long last_heartbeat;
		boolean alive;
		
		public NodeInfo(String id, NodeType type, String address){
			this.id = id;
			this.type = type;
			this.address = address;
			
			this.last_heartbeat = 0;
			this.alive = false;
		}
		
		
		public boolean isAlive(){
			return alive;
		}
		
		public boolean refreshStatus(long time_out){
			if(System.currentTimeMillis() - last_heartbeat > time_out){
				alive = false;
			}else{
				alive = true;
			}
			
			return alive;
		}
		
		
		public void logHeartbeat(){
			last_heartbeat = System.currentTimeMillis();
		}
	}
	
	
	//=========================================================================
	//================		Members of the class				===============
	//=========================================================================
	
	static final Logger logger = LoggerFactory.getLogger(OrchestratorDB.class);
	public int port;
	public NodeInfo active_pm;
	Hashtable<String, NodeInfo> nodes;
	public int time_out;
	public int refresh_rate;
	public String id;
	
	
	//=========================================================================
	//====================		Public Methods				===================
	//=========================================================================
	
	public OrchestratorDB(String id, String config_file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		logger.info("Initalizing Orchestrator DB");
		this.id = id;
		initConfig(config_file);
	}
	
	
	/**
	 * Atomically return the active PM address
	 */
	public String getActivePM(){
		String current_pm = "";
		synchronized(nodes){
			if(active_pm != null){
				current_pm = active_pm.address;
			}
			logger.debug("getActivePM: {}", current_pm);
		}
		
		return current_pm;
	}
	
	
	/**
	 * Atomically set the active PM address
	 */
	public void setActivePM(NodeInfo new_pm){
		synchronized(nodes){
			active_pm = new_pm;
			logger.debug("setActivePM: {}", new_pm);
		}
	}
	
	
	
	public void logHeartbeat(String id){
		NodeInfo node = nodes.get(id);
		if(node == null){
			logger.warn("Attemping to log heart beat for non existing node!");
			return;
		}
		node.last_heartbeat = System.currentTimeMillis();
	}
	

	/**
	 * chooses replicas for a new table. The first replica in the returned array
	 * is the master replica
	 */
	public String[] assignTableReplicas(String table_name){
		//TODO implement properly
		//The proper implementation chooses the replicas carefully to distribute the tables evenly among the data nodes
		logger.info("Assigning replicas for {}", table_name);
		String replicas[] = new String[3];
		Set<String> nodes_ids = nodes.keySet();
		Iterator<String> id_it = nodes_ids.iterator();
		int num = 0;
		while(id_it.hasNext() && num < 3){
			String id = id_it.next();
			NodeInfo node = nodes.get(id);
			if(node.isAlive() && node.type == NodeType.DataNode){
				logger.info("Assigned node: {} for replica: {} ", id, table_name);
				replicas[num++] = node.address;
			}
		}
		
		return replicas;
	}
	
	
	
	//=========================================================================
	//================			Auxiliary Methods				===============
	//=========================================================================
			
	private void initConfig(String config_file) throws ParserConfigurationException, XPathExpressionException, SAXException, IOException{
		//Loading XML document
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		Document doc = builder.parse(config_file);		
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		//Loading port
		String dummy_str = xpath.compile("//Orchestrator[@id=" + id+ "]/port").evaluate(doc);
		port = Integer.parseInt(dummy_str);
		logger.info("Orchestrator Port: " + dummy_str);
		
		dummy_str = xpath.compile("//General/heartbeat-timeout").evaluate(doc);
		time_out = Integer.parseInt(dummy_str);
		logger.info("Heartbeat time-out: {}", dummy_str);
		
		dummy_str = xpath.compile("//Orchestrator[@id=" + id+ "]/nodes-refresh-rate").evaluate(doc);
		refresh_rate = Integer.parseInt(dummy_str);
		logger.info("Nodes refresh rate: {}", dummy_str);
		
		initNodes(doc);
	}
	
	
	private void initNodes(Document doc) throws XPathExpressionException{
		nodes = new Hashtable<String, NodeInfo>();
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		NodeList pm_nodes = (NodeList) xpath.compile("//PartitionManager").evaluate(doc,  XPathConstants.NODESET);
		for(int i = 0; i < pm_nodes.getLength(); i++){
			Element pm_node = (Element) pm_nodes.item(i);
			String id = pm_node.getAttribute("id");
			String ip = ((Element) pm_node.getElementsByTagName("ip").item(0)).getTextContent();
			String port = ((Element) pm_node.getElementsByTagName("port").item(0)).getTextContent();
			
			NodeInfo node_info = new NodeInfo(id, NodeType.PartitionManager, ip + ":" + port);
			nodes.put(id, node_info);
			
			if(active_pm == null){
				active_pm = node_info;
			}
		}
		
		NodeList dn_nodes = (NodeList) xpath.compile("//DataNode").evaluate(doc, XPathConstants.NODESET);
		for(int i = 0; i < dn_nodes.getLength(); i++){
			Element dn_node = (Element) dn_nodes.item(i);
			String id = dn_node.getAttribute("id");
			String ip = ((Element) dn_node.getElementsByTagName("ip").item(0)).getTextContent();
			String port = ((Element) dn_node.getElementsByTagName("port").item(0)).getTextContent();
			
			NodeInfo node_info = new NodeInfo(id, NodeType.DataNode, ip + ":" + port);
			nodes.put(id, node_info);
		}
	}
	
}
