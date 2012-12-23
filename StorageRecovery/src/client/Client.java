package client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import messages.ClientOPMsg;
import messages.ClientOPResult;
import messages.Message;
import messages.PMAddressMsg;
import messages.ClientOPResult.ClientOPStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import debug.DebugUtility;

import partitionManager.PartitionManager;
import utilities.NoCloseOutputStream;

public class Client {
	
	static final Logger logger = LoggerFactory.getLogger(Client.class);
	String orch_ip = null;
	int orch_port = -1;
	String pm_ip = null;
	int pm_port = -1;

	public Client(String node_id, String config_file){
		logger.info("Starting Client with parameters node_id= " + node_id + " config_file= " + config_file);
		try {
			initConfig(config_file);
		} catch (Exception e) {
			e.printStackTrace();
		}
		printManual();
		
		Scanner scanner = new Scanner( System.in );
		while(true){
			String str = scanner.nextLine();
			if(str.equals("exit")){
				break;
			}else{
				executeCommand(str);
			}
		}
	}
	
	
	
	
	
	private void executeCommand(String str){
		String[] cmds = str.split(" ");
		
		if(cmds[0].toUpperCase().equals("GETPM")){
			getPM();
		}else if(cmds[0].toUpperCase().equals("CREATE")){
			createTable(cmds[1]);
		}else if(cmds[0].toUpperCase().equals("DROP")){
			dropTable(cmds[1]);
		}else if(cmds[0].toUpperCase().equals("STORE")){
			store(cmds[1], cmds[2], cmds[3]);
		}else if(cmds[0].toUpperCase().equals("READ")){
			read(cmds[1], cmds[2]);
		}else if(cmds[0].toUpperCase().equals("DELETE")){
			delete(cmds[1], cmds[2]);
		}
		
	}
	
	
	
	private void getPM(){
		Socket socket = null;
        PrintWriter out = null;
        BufferedReader in = null;
 
        try {
            socket = new Socket(orch_ip, orch_port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //out.print(Message.MessageType.GET_PM_ADDRESS);
            out.print("GET_PM_ADDRESS" + System.getProperty("line.separator"));
            out.flush();
            
            //DebugUtility.printSocket(socket);
            JAXBContext jaxb_context = JAXBContext.newInstance(PMAddressMsg.class);
            PMAddressMsg msg = (PMAddressMsg) jaxb_context.createUnmarshaller().unmarshal(in);	
            logger.info("Active PM Address: " + msg.msg_content);
            
            String[] address = msg.msg_content.split(":");
            pm_ip = address[0];
            pm_port = Integer.parseInt(address[1]);
            
            out.close();
            in.close();
            socket.close();
        } catch (UnknownHostException e) {
            logger.error("Couldn't connect to the Orchestrator");
            return;
        } catch (IOException e) {
        	logger.error("Couldn't get I/O for the connection");
        	return;
        } catch (JAXBException e) {
			logger.error("Error unmarshling reply");
			e.printStackTrace();
		}
        
       
	}
	
	
	private void createTable(String table_name){
		logger.info("creating new table: " + table_name);
		ClientOPMsg msg = new ClientOPMsg();
		msg.type = ClientOPMsg.OperationType.CREATE_TABLE;
		msg.table_name = table_name;
		ClientOPResult result = executeOperation(msg);
		if(result == null){
			return;
		}
		logger.info("Creating new table completed");
	}
	
	private void dropTable(String table_name){
		logger.info("Dropping table: " + table_name);
		ClientOPMsg msg = new ClientOPMsg();
		msg.type = ClientOPMsg.OperationType.DROP_TABLE;
		msg.table_name = table_name;
		ClientOPResult result = executeOperation(msg);
		if(result == null){
			return;
		}
		logger.info("Dropping table completed");
	}
	
	private void store(String table_name, String key, String value){
		logger.info("Storing, table: " + table_name + " Key: " + key + " Value: " + value);
		ClientOPMsg msg = new ClientOPMsg();
		msg.type = ClientOPMsg.OperationType.STORE;
		msg.table_name = table_name;
		msg.key = key;
		msg.value = value;
		ClientOPResult result = executeOperation(msg);
		if(result == null){
			return;
		}
		logger.info("Storing value completed");
	}
	
	private void read(String table_name, String key){
		logger.info("Reading, table: " + table_name + " Key: " + key);
		ClientOPMsg msg = new ClientOPMsg();
		msg.type = ClientOPMsg.OperationType.READ;
		msg.table_name = table_name;
		msg.key = key;
		ClientOPResult result = executeOperation(msg);
		if(result == null){
			return;
		}
		logger.info("Reading value completed. Value = " + result.vlaue);
	}
	
	private void delete(String table_name, String key){
		logger.info("Deleting, table: " + table_name + " Key: " + key);
		ClientOPMsg msg = new ClientOPMsg();
		msg.type = ClientOPMsg.OperationType.DELETE;
		msg.table_name = table_name;
		msg.key = key;
		ClientOPResult result = executeOperation(msg);
		if(result == null){
			return;
		}
		logger.info("Deleting value completed");
	}
	
	
	
	private ClientOPResult executeOperation(ClientOPMsg msg){
		ClientOPResult result = null;
		if(pm_ip==null){
			logger.info("initializing PartitionManager address");
			getPM();
		}
		
		try {
			logger.info("Sending first request");
			result = executeOperationAux(msg);
			logger.info("Request succeed");
		} catch (Exception e) {
			logger.info("First request failed. Updating PartitionManager address");
			getPM();
			logger.info("Sending second request");
			try {
				result = executeOperationAux(msg);
				logger.info("Request succeed");
			} catch (Exception e1) {
				logger.error("Second request failed.");
				e1.printStackTrace();
			}
		}
		
		return result;
	}
	
	private ClientOPResult executeOperationAux(ClientOPMsg msg) throws UnknownHostException, IOException{
		Socket socket = null;
		PrintWriter out = null;
        BufferedReader in = null;
        ClientOPResult result = null;
 
        try {    
            socket = new Socket(pm_ip, pm_port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            out.print("CLIENT_OPERATION\n");
            JAXBContext jaxb_context = JAXBContext.newInstance(ClientOPMsg.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.marshal( msg, out );
			socket.shutdownOutput(); //To send EOF
			
            jaxb_context = JAXBContext.newInstance(ClientOPResult.class);
            result = (ClientOPResult) jaxb_context.createUnmarshaller().unmarshal(in);	

            out.close();
            in.close();
            socket.close();
        } catch (JAXBException e) {
			logger.error("Error unmarshling reply");
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	
private void printManual(){	
	
	
		File sourceFile = new File ("manual.txt");
		FileReader fr = null;
		try {
			fr = new FileReader(sourceFile);
			int inChar;
			System.out.println("");
			System.out.println("");
			while ((inChar = fr.read()) != -1) {
				System.out.printf("%c", inChar);
			}
			System.out.println("");
			System.out.println("");
		} catch (IOException e) {
			logger.error("Failure while reading manual file: %s\n", e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if (fr != null) {
					fr.close();
				}
			} catch (IOException e) {
				logger.error("Error closing file reader: %s\n", e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	
	private void initConfig(String config_file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		//Loading XML document
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		Document doc = builder.parse(config_file);		
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		
		
		//Loading Orchestrator address
		orch_ip = xpath.compile("//Orchestrator/ip").evaluate(doc);
		//Loading Orchestrator port
		String dummy_str = xpath.compile("//Orchestrator/port").evaluate(doc);
		orch_port = Integer.parseInt(dummy_str);
		logger.info("orch_ip = " + orch_ip + " orch_port = " + orch_port);

	}
}
