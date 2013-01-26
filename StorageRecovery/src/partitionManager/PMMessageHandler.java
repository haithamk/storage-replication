package partitionManager;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utilities.NoCloseInputStream;
import utilities.NoCloseOutputStream;
import utilities.TCPUtility;

import messages.ClientOPMsg;
import messages.DataNodesAddresses;
import messages.LogMessage;
import messages.ClientOPMsg.OperationType;
import messages.ClientOPResult;
import messages.ClientOPResult.ClientOPStatus;
import messages.LogResult;
import messages.LogResult.Status;
import messages.Message.MessageType;
import messages.RecoverDNMessage;
import messages.RecoverPMMessage;
import messages.RecoverTableMessage;

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
			case RECOVER_DN_MESSAGE:
				recoverDN();
				break;
			case RECOVER:
				recover();
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
		
	
	private void recover(){
		try {
			//Init input/output streams
			XMLEventReader xer = XMLInputFactory.newInstance().createXMLEventReader(socket.getInputStream());
			
			//Read log message
			JAXBContext jaxb_context = JAXBContext.newInstance(RecoverPMMessage.class);
			RecoverPMMessage recover_msg = (RecoverPMMessage) jaxb_context.createUnmarshaller().unmarshal(xer);
			
			
			Recovery.recover(pm_db, recover_msg);
			
        	
        	PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        	out.println(MessageType.RECOVER);
            out.println("ACK");
            out.flush();
            
            xer.close();
            out.close();
            socket.close();
		} catch (JAXBException e) {
			logger.error("Error marshling/unmarshling", e);
		} catch (IOException e) {
			logger.error("IO Error", e);
		} catch (XMLStreamException e) {
			logger.error("Error in the XML reader/writer", e);
		}			
	}
	
	
	private void recoverDN(){
		try {
			//Init input/output streams
			XMLEventReader xer = XMLInputFactory.newInstance().createXMLEventReader(socket.getInputStream());
			
			//Read RecoverDNMessage message
			JAXBContext jaxb_context = JAXBContext.newInstance(RecoverDNMessage.class);
			RecoverDNMessage recover_msg = (RecoverDNMessage) jaxb_context.createUnmarshaller().unmarshal(xer);
			
			//Close connection
			xer.close();
			socket.close();
			
			//Recovering the dead data node
			handleRecoverDN(recover_msg);
			
			logger.info("Handling request completed successfully");	
		} catch (JAXBException e) {
			logger.error("Error marshling/unmarshling", e);
		} catch (IOException e) {
			logger.error("IO Error", e);
		} catch (XMLStreamException e) {
			logger.error("Error in the XML reader/writer", e);
		}			
	}
	
	
	private void handleRecoverDN(RecoverDNMessage recover_msg){
		String dead_node = recover_msg.dead_node;
		for(int i = 0; i < recover_msg.table_names.size(); i++){
			String table_name = recover_msg.table_names.get(i);
			String new_node = recover_msg.new_nodes.get(i);
			
			String[] replicas = pm_db.replicas.get(table_name);
			String reference_replica = null;
			for(int j = 0; j< replicas.length; j++){
				if(replicas[j].equals(dead_node)){
					replicas[j] = new_node;
				}else{
					reference_replica = replicas[j];
				}
			}
			
			sendRecoverMessage(table_name, reference_replica, new_node);
		}
		
		
	}
	
	private void sendRecoverMessage(String table_name, String reference_replica, String new_replica){
		Socket socket = null;
		PrintWriter out = null;
        try {   
        	//Create socket to the DN
        	String ip = new_replica.split(":")[0];
        	int port = Integer.parseInt(new_replica.split(":")[1]);
            socket = new Socket(ip, port);
            
            //Init output streams
            out = new PrintWriter(new NoCloseOutputStream(socket.getOutputStream()), true);
            
            //Send operation type
            out.println(MessageType.NEW_TABLE);
            out.flush();
            
            //Send RecoverTableMessage message
            XMLStreamWriter xsw = XMLOutputFactory.newInstance().createXMLStreamWriter(socket.getOutputStream()); 
            RecoverTableMessage recover_message = new RecoverTableMessage(table_name, reference_replica);
            JAXBContext jaxb_context = JAXBContext.newInstance(RecoverTableMessage.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.setProperty(Marshaller.JAXB_FRAGMENT,true);
			m.marshal(recover_message,xsw);
	        xsw.flush();    // send it now

            //Close connection
            xsw.close();
            out.close();
            socket.close();
        } catch (JAXBException e) {
			logger.error("Error marshling/unmarshling", e);
		} catch (UnknownHostException e) {
			logger.error("Error communicating with the remote node", e);
		} catch (IOException e) {
			logger.error("Error communicating with the remote node", e);
		} catch (XMLStreamException e) {
			logger.error("Error in the XML reader/writer", e);
		} catch (FactoryConfigurationError e) {
			logger.error("Error in the XML reader/writer", e);
		}
	}
	
	
	
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
			
			if(msg.type != OperationType.READ){
				//Read operation doesn't need to be logged in the Data Nodes
				LogResult log_result = logOperation(msg); 
				if(log_result.status != Status.SUCCESS){
					result.status = ClientOPStatus.DATA_NODE_FAIL;
					return result;
				}
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
			
			java.util.Arrays.sort(replicas.addresses);
			String arr[] = {replicas.addresses[0], replicas.addresses[1]};
			//log_message.replicas = replicas.addresses;
			log_message.replicas = arr;
			pm_db.replicas.put(msg.table_name, replicas.addresses);
			log_message.operation = LogMessage.OperationType.CREATE_TABLE;
		}else if(msg.type == OperationType.DROP_TABLE){
			freeReplicas(msg.table_name);
			log_message.operation = LogMessage.OperationType.DROP_TABLE;
		}else if(msg.type == OperationType.STORE){
			log_message.key = msg.key;
			log_message.value = msg.value;
			log_message.operation = LogMessage.OperationType.STORE;
		}else if(msg.type == OperationType.DELETE){
			log_message.key = msg.key;
			log_message.operation = LogMessage.OperationType.DELETE;
		}else if(msg.type == OperationType.READ){
			logger.warn("Atempting to log read operation", msg.type);
		}else{
			logger.warn("Partition manager recieved unknown message type: {}", msg.type);
		}
		
		
		log_message.replicas = getReplicas(log_message.table_name);
		return sendLogMessage(log_message);
	}
	
	
	
	/**
	 * Send the LogMessage to the given address via TCP connection. Returns the
	 * result received from the remote node. Or an empty result with the error
	 * code if an error occurs
	 */
	private LogResult sendLogMessage(LogMessage log_message){
		Socket socket = null;
		PrintWriter out = null;
        LogResult result = null;
        
        try {   
        	
        	//Create socket to the DN
        	String ip = log_message.replicas[0].split(":")[0];
        	int port = Integer.parseInt(log_message.replicas[0].split(":")[1]);
            socket = new Socket(ip, port);
            
            //Init output streams
            out = new PrintWriter(new NoCloseOutputStream(socket.getOutputStream()), true);
            
            //Send operation type
            out.println(MessageType.LOG_OPERATION);
            out.flush();
            
            //Send log message
            XMLStreamWriter xsw = XMLOutputFactory.newInstance().createXMLStreamWriter(socket.getOutputStream()); 
            JAXBContext jaxb_context = JAXBContext.newInstance(LogMessage.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.setProperty(Marshaller.JAXB_FRAGMENT,true);
			m.marshal(log_message,xsw);
	        xsw.flush();    // send it now

	        //get response
	        //Init input stream
	        XMLEventReader xer = XMLInputFactory.newInstance().createXMLEventReader(socket.getInputStream());
            jaxb_context = JAXBContext.newInstance(LogResult.class);
            result = (LogResult) jaxb_context.createUnmarshaller().unmarshal(xer);	

            //Close connection
            xsw.close();
            xer.close();
            out.close();
            socket.close();
        } catch (JAXBException e) {
			logger.error("Error marshling/unmarshling", e);
		} catch (UnknownHostException e) {
			logger.error("Error communicating with the remote node", e);
		} catch (IOException e) {
			logger.error("Error communicating with the remote node", e);
		} catch (XMLStreamException e) {
			logger.error("Error in the XML reader/writer", e);
		} catch (FactoryConfigurationError e) {
			logger.error("Error in the XML reader/writer", e);
		}
		
		return result;
	}
	
	
	/**
	 * Returns the master replica of the given table 
	 */
	private String[] getReplicas(String table_name){
		String[] replicas = pm_db.replicas.get(table_name);
		if(replicas == null){
			//TODO get the master address from the Orchestrator
		}
		return replicas;
	}
	
	
	/**
	 * Contacts the Orchestrator to assign new replicas for the given table
	 */
	private DataNodesAddresses assignReplicas(String table_name){
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
