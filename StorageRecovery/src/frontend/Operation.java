package frontend;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import messages.ClientOPMsg;
import messages.ClientOPResult;
import messages.PMAddressMsg;
import messages.ClientOPResult.ClientOPStatus;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Operation extends ServerResource{

	static final Logger logger = LoggerFactory.getLogger(Operation.class);
	
	public static String orch_ip = null;
	public static int orch_port = -1;
	public static String pm_ip = null;
	public static int pm_port = -1;
	
	 @Get
	    public String represent() {
	        return "hello, world";
	    }
	 
	 
	 public void getPM(){
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
		
		
		public boolean createTable(String table_name){
			logger.info("creating new table: " + table_name);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.CREATE_TABLE;
			msg.table_name = table_name;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return false;
			}
			logger.info("Creating new table completed");
			return true;
		}
		
		public boolean dropTable(String table_name){
			logger.info("Dropping table: " + table_name);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.DROP_TABLE;
			msg.table_name = table_name;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return false;
			}
			logger.info("Dropping table completed");
			return false;
		}
		
		public boolean store(String table_name, String key, String value){
			logger.info("Storing, table: " + table_name + " Key: " + key + " Value: " + value);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.STORE;
			msg.table_name = table_name;
			msg.key = key;
			msg.value = value;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return false;
			}
			logger.info("Storing value completed");
			return true;
		}
		
		public boolean read(String table_name, String key){
			logger.info("Reading, table: " + table_name + " Key: " + key);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.READ;
			msg.table_name = table_name;
			msg.key = key;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return false;
			}
			logger.info("Reading value completed. Value = " + result.vlaue);
			return false;
		}
		
		public boolean delete(String table_name, String key){
			logger.info("Deleting, table: " + table_name + " Key: " + key);
			ClientOPMsg msg = new ClientOPMsg();
			msg.type = ClientOPMsg.OperationType.DELETE;
			msg.table_name = table_name;
			msg.key = key;
			ClientOPResult result = executeOperation(msg);
			if(result == null || result.status != ClientOPStatus.SUCCESS){
				return false;
			}
			logger.info("Deleting value completed");
			return true;
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
