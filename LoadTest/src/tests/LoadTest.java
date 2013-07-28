package tests;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class LoadTest {

	
	//=========================================================================
	//================		Members of the class				===============
	//=========================================================================
	
	private int min_clients;
	private int max_clients;
	private int min_data_nodes;
	private int max_data_nodes;
	private int requests_number;
	private int tables_number;
	
	private String server_address;
	private LinkedList<String> requests_base;
	private LinkedList<String> results;
	
	
	//=========================================================================
	//====================		Public Methods				===================
	//=========================================================================
	
	/**
	 * Main function
	 */	
	public static void main(String[] args) {
		try{
			if(args.length == 0){
				System.out.println("Please set the config file path");
				return;
			}
			String config_file = args[0];
			LoadTest load_test = new LoadTest();
			load_test.initTestParameters(config_file);
			load_test.runTests();
		}catch(Exception e){
			System.out.println("Failed to run the test");
			e.printStackTrace();
		}
		
	}
	
	
	//=========================================================================
	//================			Auxiliary Methods				===============
	//=========================================================================
	
	/**
	 * Runs the tests as configured and reports the results
	 * @throws InterruptedException 
	 */
	private void runTests() throws InterruptedException{
		results = new LinkedList<String>();
		createRequests();

		for(int data_nodes_num=min_data_nodes; data_nodes_num<=max_data_nodes; data_nodes_num++){
			for(int clients_num=min_clients; clients_num<= max_clients; clients_num++){
				reset(data_nodes_num);
				double result = runTest(clients_num);
				results.add(String.format("%d:%d:%f", data_nodes_num, clients_num, result));
			}
		}
		
		System.out.println("\n\n\n\n\n");
		System.out.println("===============================   Final Results    ===============================");
		for(int i=0; i<results.size(); i++){
			System.out.println(results.get(i));
		}
		
	}
	
	
	/**
	 * Runs one test with a given number of clients
	 */
	private double runTest(int clients_num) throws InterruptedException{
		ConcurrentLinkedQueue<String> requests = new ConcurrentLinkedQueue<String>();
		ConcurrentLinkedQueue<Double> results = new ConcurrentLinkedQueue<Double>();
		
		requests.addAll(requests_base);
		ExecutorService pool = Executors.newFixedThreadPool(clients_num);
		
		for(int i=0; i<clients_num; i++){
			pool.execute(new OperationsSender(requests, results));
		}
		
		pool.awaitTermination(10, TimeUnit.DAYS);
		
		int count = 0;
		double sum = 0;
		for(Double avg_time : results){
			count++;
			sum += avg_time;
		}
		
		return (sum/count);
	}
	
	/**
	 * Send a reqeset request that resets the whole systems
	 */
	private void reset(int data_nodes_num){
		//TODO
	}
	
	//================			Create requests Methods				===============
	/**
	 * Creates the queue of the messages to requests to be sent to the Storage Replication service
	 */
	private void createRequests(){
		requests_base = new LinkedList<String>();
		LinkedList<String> keys = new LinkedList<String>();
		
		for(int i=0; i<tables_number; i++){
			String new_table_cmd = String.format("%s/Create/Table/%d", server_address, i);
			requests_base.addLast(new_table_cmd);
		}
		
	    Random randomGenerator = new Random();
		for(int i=0; i<requests_number; i++){
			String new_cmd = null;
			int rand = randomGenerator.nextInt();
			switch(rand % 5){
				case 0:
				case 1: new_cmd = createAddOperation(rand, keys); break;
				case 2:
				case 3: new_cmd = createReadOperation(rand, keys); break;
				case 4: new_cmd = createDeleteOperation(rand, keys); break;
			}
			
			if(new_cmd != null){
				requests_base.addLast(new_cmd);
			}
		}
	}
	
	/**
	 * Returns a http url of a add new value operation
	 */
	private String createAddOperation(int rand, LinkedList<String> keys){
		UUID uuid = UUID.randomUUID();
		int table = rand % tables_number;
		keys.add(String.format("%d:%l", table, uuid.getMostSignificantBits()));
		return String.format("%s/Put/%d/%l/%l", server_address, table, uuid.getMostSignificantBits(), uuid.getMostSignificantBits());
	}
	
	
	/**
	 * Returns a http url of a reading existing value operation
	 */
	private String createReadOperation(int rand, LinkedList<String> keys){
		if(keys.size() == 0)
			return null;
		
		int index = rand % keys.size();
		String table = keys.get(index).split(":")[0];
		String key = keys.get(index).split(":")[1];
		return String.format("%s/Read/%s/%s", server_address, table, key);
	}
	
	/**
	 * Returns a http url of a delete existing value operation
	 */
	private String createDeleteOperation(int rand, LinkedList<String> keys){
		if(keys.size() == 0)
			return null;
		
		int index = rand % keys.size();
		String table = keys.get(index).split(":")[0];
		String key = keys.get(index).split(":")[1];
		keys.remove(index);
		return String.format("%s/Delete/Value/%s/%s", server_address, table, key);
	}

	
	//==================			Init Methods				=================
	
	/**
	 * Reads and initializes the test parameters from the XML configuration file
	 */
	private void initTestParameters(String config_file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		//Loading XML document
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		Document doc = builder.parse(config_file);		
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		//Loading port
		String dummy_str = xpath.compile("//min_clients").evaluate(doc);
		min_clients = Integer.parseInt(dummy_str);

		dummy_str = xpath.compile("//max_clients").evaluate(doc);
		max_clients = Integer.parseInt(dummy_str);
		
		dummy_str = xpath.compile("//min_data_nodes").evaluate(doc);
		min_data_nodes = Integer.parseInt(dummy_str);
		
		dummy_str = xpath.compile("//max_data_nodes").evaluate(doc);
		max_data_nodes = Integer.parseInt(dummy_str);
		
		dummy_str = xpath.compile("//requests_number").evaluate(doc);
		requests_number = Integer.parseInt(dummy_str);
		
		dummy_str = xpath.compile("//tables_number").evaluate(doc);
		tables_number = Integer.parseInt(dummy_str);
	}

}
