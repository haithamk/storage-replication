package orchestrator;

import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class OrchestratorDB {
	public int port;
	public String active_pm_address;
	
	
	
	
	public OrchestratorDB(String config_file, String id) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		
		//Loading XML document
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		Document doc = builder.parse(config_file);
		
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		String port_str = xpath.compile("//Orchestrator[@id=1]/clients-port").evaluate(doc);

		port = Integer.parseInt(port_str);
		
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
		}
		return current_pm;
	}
	
	/**
	 * Atomically set the active PM address
	 */
	public void setActivePM(String new_pm){
		synchronized(active_pm_address){
			active_pm_address = new_pm;
		}
	}
	
}
