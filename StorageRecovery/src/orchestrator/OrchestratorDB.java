package orchestrator;

import java.io.IOException;
import java.util.Hashtable;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class OrchestratorDB {
	
	class NodeInfo{
		String id;
		String address;
		int port;
		long last_heartbeat;
	}
	
	
	static final Logger logger = LoggerFactory.getLogger(OrchestratorDB.class);
	public int port;
	public NodeInfo active_pm;
	Hashtable<String, NodeInfo> nodes;
	
	
	
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
		String port_str = xpath.compile("//Orchestrator[@id=" + id+ "]/port").evaluate(doc);
		port = Integer.parseInt(port_str);
		logger.info("Orchestrator Port: " + port_str);
		
		//TODO temporarily for testing proposes. 
		active_pm = new NodeInfo();
		active_pm.address = "127.0.0.1:43010";
		
		
		nodes = new Hashtable<String, NodeInfo>();
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
		node.last_heartbeat = System.currentTimeMillis();
	}
	
}
