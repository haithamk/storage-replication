package partitionManager;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import orchestrator.OrchMessageHandler;

import org.w3c.dom.Document;


import shared.NodeAddress;
import shared.TableInfo;

public class PartitionManager {
	
	PartitionManagerDB pm_db;
	private ExecutorService pool;
	private ServerSocket commSocket;
	
	
	public PartitionManager(String config_file){
		try{
			initDB(config_file);
			
			pool = Executors.newCachedThreadPool();
			commSocket = new ServerSocket(pm_db.port);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	

	
	
	
	
	public void run(){
		Socket socket = null;
		
		while(true){
			try {
				socket = commSocket.accept();
				//Execute method is NOT blocking function. The Job is saved and when
				//There are available thread it will handle it.
				pool.execute(new PMMessageHandler(socket, pm_db));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	private void initDB(String config_file) throws Exception{
		pm_db = new PartitionManagerDB();
		
		//Loading XML document
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		Document doc = builder.parse(config_file);
		
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		String port_str = xpath.compile("//port").evaluate(doc);

		pm_db.port = Integer.parseInt(port_str);
	}

}
