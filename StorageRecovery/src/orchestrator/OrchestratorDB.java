package orchestrator;

import java.io.IOException;

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
	
	static final Logger logger = LoggerFactory.getLogger(OrchestratorDB.class);
	public int port;
	public String active_pm_address;
	
	
	
	
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
		String port_str = xpath.compile("//Orchestrator[@id=1]/clients-port").evaluate(doc);
		port = Integer.parseInt(port_str);
		logger.info("Orchestrator Port: " + port);
		
		//TODO temporarily for testing proposes. 
		active_pm_address = "192.36.45.5:45";
		
	}
	/**
	 * Atomically return the active PM address
	 */
	public String getActivePM(){
		String current_pm;
		synchronized(active_pm_address){
			current_pm = active_pm_address;
			logger.debug("getActivePM: {}", current_pm);
		}
		
		return current_pm;
	}
	
	/**
	 * Atomically set the active PM address
	 */
	public void setActivePM(String new_pm){
		synchronized(active_pm_address){
			active_pm_address = new_pm;
			logger.debug("setActivePM: {}", new_pm);
		}
	}
	
}
