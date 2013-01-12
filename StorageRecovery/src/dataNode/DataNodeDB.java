package dataNode;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class DataNodeDB {

	
	private static final Logger logger = LoggerFactory.getLogger(DataNodeDB.class);
	
	public String id;				//Node id	
	public int port;				//Node operating port
	public String orch_ip;
	public int orch_port;
	public int heartbeat_rate;
	public String work_dir;			//Directory to save XML files

	
	public DataNodeDB(String id, String config_file) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException{
		logger.info("Initalizing DataNode DB");
		this.id = id;
		initConfig(config_file);
		
		File work_file = new File(work_dir);
		FileUtils.deleteDirectory(work_file);
		work_file.mkdir();
	}
	
	private void initConfig(String config_file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		//Loading XML document
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		Document doc = builder.parse(config_file);		
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		//Loading port
		String dummy_str = xpath.compile("//DataNode[@id =" + id + "]/port").evaluate(doc);
		port = Integer.parseInt(dummy_str);
		logger.info(" PartitionManagerDB({}) Port: {}", id, port);
		
		//Loading Orchestrator address
		orch_ip = xpath.compile("//Orchestrator/ip").evaluate(doc);
		//Loading Orchestrator port
		dummy_str = xpath.compile("//Orchestrator/port").evaluate(doc);
		orch_port = Integer.parseInt(dummy_str);
		logger.info("Orchestrator IP: {}, port: {}", orch_ip, orch_port);
		
		//Loading heartbeat rate
		dummy_str = xpath.compile("//General/heartbeat-rate").evaluate(doc);
		heartbeat_rate = Integer.parseInt(dummy_str);
		logger.info("Heart beat rate: {}", heartbeat_rate);
		
		//Setting the work dir to save the persistent files
		work_dir =  xpath.compile("//DataNodes/WorkDir").evaluate(doc).trim() + id + "/";
		
	}
}
