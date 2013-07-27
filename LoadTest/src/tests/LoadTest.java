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

	private int min_clients_number;
	private int max_clients_number;
	private int requests_number;
	private int tables_number;
	
	private String server_address;
	private LinkedList<String> requests;
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
	}
	
	private void runTests(){
		for(int clients_num=min_clients_number; clients_num<= max_clients_number; clients_num++){
			reset();
		}
	}
	
	
	private void runTest(int clients_num) throws InterruptedException{
		createRequests();
		runClients(clients_num);
	}
	
	private void reset(){
		
	}
	
	private double runClients(int clients_number) throws InterruptedException{
		ConcurrentLinkedQueue<String> requests = new ConcurrentLinkedQueue<String>();
		ConcurrentLinkedQueue<Double> results = new ConcurrentLinkedQueue<Double>();
		
		ExecutorService pool = Executors.newFixedThreadPool(clients_number);
		
		for(int i=0; i<clients_number; i++){
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
	
	private void createRequests(){
		requests = new LinkedList<String>();
		LinkedList<String> keys = new LinkedList<String>();
		
		for(int i=0; i<tables_number; i++){
			String new_table_cmd = String.format("%s/Create/Table/%d", server_address, i);
			requests.addLast(new_table_cmd);
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
				requests.add(new_cmd);
			}
		}
	}
	
	private String createAddOperation(int rand, LinkedList<String> keys){
		UUID uuid = UUID.randomUUID();
		int table = rand % tables_number;
		keys.add(String.format("%d:%l", table, uuid.getMostSignificantBits()));
		return String.format("%s/Put/%d/%l/%l", server_address, table, uuid.getMostSignificantBits(), uuid.getMostSignificantBits());
	}
	
	private String createReadOperation(int rand, LinkedList<String> keys){
		if(keys.size() == 0)
			return null;
		
		int index = rand % keys.size();
		String table = keys.get(index).split(":")[0];
		String key = keys.get(index).split(":")[1];
		return String.format("%s/Read/%s/%s", server_address, table, key);
	}
	
	private String createDeleteOperation(int rand, LinkedList<String> keys){
		if(keys.size() == 0)
			return null;
		
		int index = rand % keys.size();
		String table = keys.get(index).split(":")[0];
		String key = keys.get(index).split(":")[1];
		keys.remove(index);
		return String.format("%s/Delete/Value/%s/%s", server_address, table, key);
	}

	
	//Reads and initializes the test parameters from the XML configuration file
	private void initTestParameters(String config_file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		//Loading XML document
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		Document doc = builder.parse(config_file);		
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		//Loading port
		String dummy_str = xpath.compile("//Orchestrator[@id=" + "id" + "]/port").evaluate(doc);
//		port = Integer.parseInt(dummy_str);
//		logger.info("Orchestrator Port: " + dummy_str);
	}

}
