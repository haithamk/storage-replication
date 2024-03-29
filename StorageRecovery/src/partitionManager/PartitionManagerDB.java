package partitionManager;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

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

import partitionManager.DBException.Cause;

public class PartitionManagerDB {
	
	
	//=========================================================================
	//================		Members of the class				===============
	//=========================================================================
	
	
	static final Logger logger = LoggerFactory.getLogger(PartitionManagerDB.class);
	public String node_id;
	private Map<String, Table> tables;	
	public Hashtable<String, String[]> replicas;
	public int port;
	public String orch_ip;
	public int orch_port;
	public int heartbeat_rate;
	

	//=========================================================================
	//================			Public Methods					===============
	//=========================================================================
	public PartitionManagerDB(String node_id, String config_file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		logger.info("Initalizing PartitionManagerDB({}) DB", node_id);
		this.node_id = node_id;
		initConfig(config_file);
		//Init DB variables
		tables = new Hashtable<String, Table>(); //TODO check concurrent accesses to this data base??, ConcurrentHashMap???
		replicas = new Hashtable<String, String[]>();
	}
	
	/**
	 * Resets the DB
	 */
	public void reset(){
		tables.clear();
		replicas.clear();
	}
	
	/**
	 * Create a new empty table with the given name. If another table with the
	 * same name already exist it will be over write
	 * 
	 * @param table_name the name of the new table
	 */
	public void createTable(String table_name){
		logger.debug("Creating table: {}", table_name);
		Table table = new Table(table_name);
		//tables.put(table_name, table);
		addTable(table_name, table);
	}
	
	
	public void addTable(String table_name, Table table){
		//TODO implement, check concurency issues
		tables.put(table_name, table);
	}
	

	public void dropTable(String table_name){
		logger.debug("Dropping table: {}", table_name);
		tables.remove(table_name);
	}
	
	
	public void store(String table_name, String key, String value) throws DBException{
		logger.debug("Storing to tbale name: {}. key: {} value: {}", new Object[]{table_name, key, value});
		Table table = tables.get(table_name);
		if(table == null){
			logger.warn("Attempted to store to non existing table: {}", table_name);
			throw new DBException(Cause.TABLE_DOESNT_EXIST);
		}
		
		table.store(key, value);
	}
	
	
	public String read(String table_name, String key) throws DBException{
		logger.debug("Reading from table: {}. key: {}", table_name, key);
		Table table = tables.get(table_name);
		if(table == null){
			logger.warn("Attempted to read from non existing table: {}", table_name);
			throw new DBException(Cause.TABLE_DOESNT_EXIST);
		}
		String result =  table.read(key); 
		return result;
	}
	
	public void delete(String table_name, String key) throws DBException{
		logger.debug("Deleting from table: {}. key: {}", table_name, key);
		Table table = tables.get(table_name);
		if(table == null){
			logger.warn("Attempted to delete from non existing table: {}", table_name);
			throw new DBException(Cause.TABLE_DOESNT_EXIST);
		}
		table.delete(key); 
	}
	
	
	
	//=========================================================================
	//================			Auxiliary Methods				===============
	//=========================================================================
		
	private void initConfig(String config_file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		//Loading XML document
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		Document doc = builder.parse(config_file);		
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		//Loading port
		String dummy_str = xpath.compile("//PartitionManager[@id =" + node_id + "]/port").evaluate(doc);
		port = Integer.parseInt(dummy_str);
		logger.info(" PartitionManagerDB({}) Port: {}", node_id, port);
		
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
	}
	
	
	
	
}
