package frontend;

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
import messages.PMAddressMsg;
import messages.ClientOPResult.ClientOPStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import org.restlet.*;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;


public class FrontEnd extends Component{

	//=========================================================================
	//================		Members of the class				===============
	//=========================================================================
		
	static final Logger logger = LoggerFactory.getLogger(FrontEnd.class);
	String orch_ip = null;
	int orch_port = -1;
	String pm_ip = null;
	int pm_port = -1;
	
	int http_port = 8182;
	boolean isServer = true;
	

	
	//=========================================================================
	//====================		Public Methods				===================
	//=========================================================================
	public FrontEnd(String node_id, String config_file){
		logger.info("Starting Client with parameters node_id= " + node_id + " config_file= " + config_file);
		try {
			initConfig(config_file);
		} catch (Exception e) {
			e.printStackTrace();
		}
		printManual();
	}
	
	
	public void run(){
		if(isServer){
			createServer();
		}else{
			getCommandfromCL();
		}	
	}
	
	//=========================================================================
	//===============		Auxiliary Operating Methods			===============
	//=========================================================================
	
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
	
	
	// ================== Server methods ==================

	
	private void createServer(){
		try {
			new Server(Protocol.HTTP, http_port, this).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	@Override
	public void handle(Request request, Response response){
		super.handle(request, response);
		
		String result = executeCommandFromRequest(request.getResourceRef().getRemainingPart());
		response.setEntity(result, MediaType.TEXT_PLAIN);  
	}
	
	
	private String executeCommandFromRequest(String str){
		String result = null;
		String[] cmds = str.split("/");
		
		try{
			if(cmds[0].toUpperCase().equals("GETPM")){
				// /GetPM
				result = getPM();
			}else if(cmds[0].toUpperCase().equals("CREATE")){
				// /Create/Table/<table-name>
				result = createTable(cmds[2]);
			}else if(cmds[0].toUpperCase().equals("DELETE") && cmds[1].toUpperCase().equals("TABLE")){
				// /Delete/Table/<table-name>
				result = dropTable(cmds[2]);
			}else if(cmds[0].toUpperCase().equals("PUT")){
				// /Put/<table-name>/<key>/<value>
				result = store(cmds[1], cmds[2], cmds[3]);
			}else if(cmds[0].toUpperCase().equals("READ")){
				// /Read/<table-name>/<key>
				result = read(cmds[1], cmds[2]);
			}else if(cmds[0].toUpperCase().equals("DELETE") && cmds[1].toUpperCase().equals("TABLE")){
				// /Delete/Value/<tbale-name>/<key>
				result = delete(cmds[2], cmds[3]);
			}
		}catch(Exception e){
			e.printStackTrace();
			result = null;
		}
		
		return result;
	}
	
	// ================== Command Line methods ==================
	
	private void getCommandfromCL(){
		Scanner scanner = null;
		try {
			scanner = new Scanner( System.in );
			while(true){
				String str = scanner.nextLine();
				if(str.equals("exit")){
					break;
				}else{
					executeCommandFromCL(str);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(scanner != null)
				scanner.close();
		}
	}
	
	private void executeCommandFromCL(String str){
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
	//=========================================================================
	//================			Operation Methods				===============
	//=========================================================================
	
	
	 	private String getPM(){
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
	            
	            if(!msg.msg_content.equals("")){
	            	 String[] address = msg.msg_content.split(":");
	                 pm_ip = address[0];
	                 pm_port = Integer.parseInt(address[1]);
	            }else{
	            	pm_ip = null;
	            	pm_port = -1;
	            }
	           
	            out.close();
	            in.close();
	            socket.close();
	            
	            return msg.msg_content;
	        } catch (UnknownHostException e) {
	            logger.error("Couldn't connect to the Orchestrator");        
	        } catch (IOException e) {
	        	logger.error("Couldn't get I/O for the connection");
	        } catch (JAXBException e) {
				logger.error("Error unmarshling reply");
				e.printStackTrace();
			}
	        
	        return "ERROR";
		}
		
		private String createTable(String table_name){
			logger.info("creating new table: " + table_name);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.CREATE_TABLE;
			msg.table_name = table_name;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return "ERROR";
			}
			logger.info("Creating new table completed");
			return "SUCCESS";
		}
		
		private String dropTable(String table_name){
			logger.info("Dropping table: " + table_name);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.DROP_TABLE;
			msg.table_name = table_name;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return "ERROR";
			}
			logger.info("Dropping table completed");
			return "SUCCESS";
		}
		
		private String store(String table_name, String key, String value){
			logger.info("Storing, table: " + table_name + " Key: " + key + " Value: " + value);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.STORE;
			msg.table_name = table_name;
			msg.key = key;
			msg.value = value;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return "ERROR";
			}
			logger.info("Storing value completed");
			return "SUCCESS";
		}
		
		private String read(String table_name, String key){
			logger.info("Reading, table: " + table_name + " Key: " + key);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.READ;
			msg.table_name = table_name;
			msg.key = key;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return "ERROR";
			}
			logger.info("Reading value completed. Value = " + result.vlaue);
			return result.vlaue;
		}
		
		private String delete(String table_name, String key){
			logger.info("Deleting, table: " + table_name + " Key: " + key);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.DELETE;
			msg.table_name = table_name;
			msg.key = key;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return "ERROR";
			}
			logger.info("Deleting value completed");
			return "SUCCESS";
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
	        	logger.error("Error marshling/unmarshling", e);
			}
			
			return result;
		}
	
}
