package orchestrator;

import java.io.IOException;
import java.util.Hashtable;

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
		
		public NodeInfo(){
			
		}
		
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
			
			
//			synchronized (id) {
//				
//			}
		}
		
		
		public void logHeartbeat(){
			last_heartbeat = System.currentTimeMillis();
//			synchronized (id) {
//				
//			}
		}
	}
	
	
	static final Logger logger = LoggerFactory.getLogger(OrchestratorDB.class);
	public int port;
	public NodeInfo active_pm;
	Hashtable<String, NodeInfo> nodes;
	public int time_out;
	public int refresh_rate;
	
	
	
	public OrchestratorDB(String id, String config_file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		logger.info("Initalizing Orchestrator DB");
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
			String port = ((Element)pm_node.getElementsByTagName("port").item(0)).getTextContent();
			
			NodeInfo node_info = new NodeInfo(id, NodeType.PartitionManager, ip + ":" + port);
			nodes.put(id, node_info);
			
			if(active_pm == null){
				active_pm = node_info;
			}
			
		}
		
		
		
	}
	
	
	/**
	 * Atomically return the active PM address
	 */
	public String getActivePM(){
		String current_pm;
		synchronized(nodes){
			current_pm = active_pm.address;
			logger.debug("getActivePM: {}", current_pm);
		}
		
		return current_pm;
	}
	
	/**
	 * Atomically set the active PM address
	 */
	public void setActivePM(String new_pm){
		synchronized(nodes){
			active_pm.address = new_pm;
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
	
}
