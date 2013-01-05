package partitionManager;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utilities.NoCloseInputStream;

import messages.ClientOPMsg;
import messages.DataNodesAddresses;
import messages.LogMessage;
import messages.ClientOPMsg.OperationType;
import messages.ClientOPResult;
import messages.ClientOPResult.ClientOPStatus;
import messages.LogResult;
import messages.LogResult.Status;
import messages.Message.MessageType;

public class PMMessageHandler implements Runnable {

	
	//=========================================================================
	//================		Members of the class				===============
	//=========================================================================
	
	static final Logger logger = LoggerFactory.getLogger(PMMessageHandler.class);
	Socket socket;
	PartitionManagerDB pm_db;
	
	
	
	//=========================================================================
	//====================		Public Methods				===================
	//=========================================================================
	public PMMessageHandler(Socket socket, PartitionManagerDB pm_db) {
		logger.debug("New PMMessageHandler created");
		this.socket = socket;
		this.pm_db = pm_db;
	}

	@Override
	public void run() {
		try {
			
			DataInputStream inputReader = new DataInputStream(socket.getInputStream());
			@SuppressWarnings("deprecation")
			MessageType type = MessageType.valueOf(inputReader.readLine());
			logger.info("Handling message of type: {}", type);
			
			switch(type){
			case CLIENT_OPERATION:
				handleClientOperation();
				break;
			default:
				break;					
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	//=========================================================================
	//================			Auxiliary Methods				===============
	//=========================================================================
		
	/**
	 * Unmarshalls the Client request, executes it and returns a result to the user.
	 */
	private void handleClientOperation(){
		try{
			NoCloseInputStream in = new NoCloseInputStream(socket.getInputStream());
			JAXBContext jaxb_context = JAXBContext.newInstance(ClientOPMsg.class);
			ClientOPMsg msg = (ClientOPMsg) jaxb_context.createUnmarshaller().unmarshal(in);			
			logger.debug("Message headers:\n{}\nMessage content:\n{}", msg.getHeaders(), msg.toString());
			//TODO check if the check sum is fine
			//TODO check if the user is authorized
			
			ClientOPResult result = executeClientOperation(msg);
			
			//Creating marshaler
			jaxb_context = JAXBContext.newInstance(ClientOPResult.class);			
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			//Sending the result
			OutputStream out = socket.getOutputStream();
			m.marshal( result, out );
			out.flush();
			out.close();
			socket.close();
			logger.info("Handling request completed successfully");			
		} catch (IOException e) {
			logger.error("IO error occurred while handling message", e);
		} catch (JAXBException e) {
			logger.error("Jaxb error occurred while handling message", e);
		}
		
	}
	
	
	/**
	 * Executes the given operation.
	 * First the operation is logged to the Data nodes. If and only if the logging
	 * succeeded the operation will be executed locally. 
	 */
	private ClientOPResult executeClientOperation(ClientOPMsg msg){
		ClientOPResult result = new ClientOPResult();			
		try{	
			
			LogResult log_result = logOperation(msg); 
			if(log_result.status != Status.SUCCESS){
				result.status = ClientOPStatus.DATA_NODE_FAIL;
				return result;
			}
			
			if(msg.type == OperationType.CREATE_TABLE){
				pm_db.createTable(msg.table_name);
			}else if(msg.type == OperationType.DROP_TABLE){
				pm_db.dropTable(msg.table_name);
			}else if(msg.type == OperationType.STORE){
				pm_db.store(msg.table_name, msg.key, msg.value);
			}else if(msg.type == OperationType.READ){
				result.vlaue = pm_db.read(msg.table_name, msg.key);
			}else if(msg.type == OperationType.DELETE){
				pm_db.delete(msg.table_name, msg.key);
			}else{
				logger.warn("Partition manager recieved unknown message type: {}", msg.type);
			}
		}catch(DBException e){
			switch(e.cause){
			case TABLE_DOESNT_EXIST:
				result.status = ClientOPStatus.TABLE_DOESNT_EXIST;
				break;
			default:
				break;
			}
		}
		return result;
	}
	
	
	/**
	 * Logs an operation to the data nodes. The master data node is located and
	 *  the request is sent to him. 
	 */
	private LogResult logOperation(ClientOPMsg msg){
		LogMessage log_message = new LogMessage();
		log_message.table_name = msg.table_name;
		
		if(msg.type == OperationType.CREATE_TABLE){
			DataNodesAddresses replicas = assignReplicas(msg.table_name);
			
			if(replicas.addresses == null){
				logger.warn("No available Data Nodes");
				LogResult log_result = new LogResult();
				log_result.status = Status.NO_DN_AVAILABLE;
				return log_result;
			}
			log_message.replicas = replicas.addresses;
		}else if(msg.type == OperationType.DROP_TABLE){
			freeReplicas(msg.table_name);
			log_message.operation = "DROP";
		}else if(msg.type == OperationType.STORE){
			log_message.key = msg.key;
			log_message.value = msg.value;
		}else if(msg.type == OperationType.READ){
			log_message.key = msg.key;
		}else if(msg.type == OperationType.DELETE){
			log_message.key = msg.key;
		}else{
			logger.warn("Partition manager recieved unknown message type: {}", msg.type);
		}
		
		
		String master_replica = getMasterReplica(log_message.table_name);
		return sendLogMessage(master_replica, log_message);
	}
	
	
	
	/**
	 * Send the LogMessage to the given address via TCP connection. Returns the
	 * result recieved from the remote node. Or an empty result with the error
	 * code if an error occurs
	 */
	private LogResult sendLogMessage(String address, LogMessage log_message){
		Socket socket = null;
		PrintWriter out = null;
        BufferedReader in = null;
        LogResult result = null;
        
        try {   
        	String ip = address.split(":")[0];
        	int port = Integer.parseInt(address.split(":")[1]);
            socket = new Socket(ip, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            out.print(MessageType.LOG_OPERATION + "\n");
            JAXBContext jaxb_context = JAXBContext.newInstance(ClientOPMsg.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.marshal( log_message, out );
			socket.shutdownOutput(); //To send EOF
			
            jaxb_context = JAXBContext.newInstance(LogResult.class);
            result = (LogResult) jaxb_context.createUnmarshaller().unmarshal(in);	

            out.close();
            in.close();
            socket.close();
        } catch (JAXBException e) {
			logger.error("Error marshling/unmarshling", e);
		} catch (UnknownHostException e) {
			logger.error("Error communicating with the remote node", e);
		} catch (IOException e) {
			logger.error("Error communicating with the remote node", e);
		}
		
		return result;
	}
	
	
	/**
	 * Returns the master replica of the given table 
	 */
	private String getMasterReplica(String table_name){
		//TODO
		return "";
	}
	
	
	/**
	 * Contacts the Orchestrator to assign new replicas for the given table
	 */
	private DataNodesAddresses assignReplicas(String table_name){
		//TODO check
		Socket socket = null;
		PrintWriter out = null;
        BufferedReader in = null;
        DataNodesAddresses result = null;
 
        try {    
            socket = new Socket(pm_db.orch_ip, pm_db.orch_port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            //Sending request
            out.println(MessageType.GET_TABLE_REPLICAS);
            out.println(table_name);
            
            
            JAXBContext jaxb_context = JAXBContext.newInstance(DataNodesAddresses.class);
            result = (DataNodesAddresses) jaxb_context.createUnmarshaller().unmarshal(in);	

            out.close();
            in.close();
            socket.close();
        } catch (JAXBException e) {
        	logger.error("Error marshling/unmarshling", e);
		} catch (IOException e) {
			logger.error("Error with communication", e);
		}
		
		return result;
	}
	
	
	/**
	 * Contacts the Orchestrator to free the allocated replicas for the given table
	 */
	private void freeReplicas(String table_name){
		Socket socket = null;
		PrintWriter out = null;
        BufferedReader in = null;
 
        try {    
            socket = new Socket(pm_db.orch_ip, pm_db.orch_port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            //Sending request
            out.println(MessageType.FREE_REPLICAS);
            out.println(table_name);
            
            
            //TODO implement
            JAXBContext jaxb_context = JAXBContext.newInstance(DataNodesAddresses.class);
//            result = (DataNodesAddresses) jaxb_context.createUnmarshaller().unmarshal(in);	

            out.close();
            in.close();
            socket.close();
        } catch (JAXBException e) {
        	logger.error("Error marshling/unmarshling", e);
		} catch (IOException e) {
			logger.error("Error with communication", e);
		}
	}
	


}
