package dataNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import messages.LogMessage;
import messages.LogMessage.OperationType;
import messages.LogResult;
import messages.LogResult.Status;
import messages.Message.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import debug.DebugUtility;

import utilities.NoCloseInputStream;
import utilities.TCPFileUtility;
import utilities.XMLUtility;

public class DNMessageHandler implements Runnable {

	static final Logger logger = LoggerFactory.getLogger(DNMessageHandler.class);
	
	Socket socket;
	DataNodeDB dn_db;
	BufferedReader inputReader;
	
	
	public DNMessageHandler(Socket socket, DataNodeDB dn_db){
		logger.debug("New DNMessageHandler created");
		this.socket = socket;
		this.dn_db = dn_db;
	}
	
	
	@Override
	public void run() {
		try{
			inputReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			MessageType type = MessageType.valueOf(inputReader.readLine());
			logger.info("Handling message of type: {}", type.toString());
			switch (type) {
			case LOG_OPERATION: 
				handleLogOperation();
				break;
			case RECOVER:
				handleRecover();
				break;
			default:
				break;
			}
			
			
			
		} catch (IOException e) {
			logger.error("IO error occurred while handling message", e);
		}
	}
	
	
	
	/**
	 * Reads the XML file of the requested table and transfers it over socket to
	 * the destination
	 */
	private void handleRecover(){
		try{
			String table_name = inputReader.readLine();
			String file_path = dn_db.work_dir + table_name + ".xml";
			logger.info("Handling recover request for table: {} ", table_name); 
			
			//Send the given file over the socket
			TCPFileUtility.sendFile(file_path, socket);
			
			socket.close();
			logger.info("getTableReplicas result sent successfully");
		}catch(Exception e){
			logger.error("An error occurred in the method getTableReplicas", e);
		}
		
	}
	
	
	/**
	 * Logs an operation to the persistent disk
	 */
	private void handleLogOperation(){
		try {
			LogResult result = null;
			
//			DebugUtility.printSocket(socket);
			
			JAXBContext jaxb_context = JAXBContext.newInstance(LogMessage.class);
			LogMessage log_msg = (LogMessage) jaxb_context.createUnmarshaller().unmarshal(inputReader);			
			
			//Log operation in persistent disk
			result = logOperation(log_msg);
			
			//Creating marshaler
			jaxb_context = JAXBContext.newInstance(LogResult.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			//Sending the result
			OutputStream out = socket.getOutputStream();
			m.marshal( result, out );
			out.flush();
			out.close();
			socket.close();
			logger.info("Handling request completed successfully");	
		} catch (JAXBException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}			
		
	}
	
	
	private LogResult logOperation(LogMessage log_message){
		
		LogResult result = null;
		
		if( (++log_message.count) < log_message.replicas.length){
			logger.info("forward the request to the next replica");
			LogResult forward_result = forwardRequest(log_message);
			if(forward_result.status != Status.SUCCESS){
				logger.warn("Error in forwarding the request to the next replica");
				return forward_result;
			}
			logger.info("Request forwarded to the next replica successfully");
		}
		
		logger.info("Logging the message locally");
		String file_path = dn_db.work_dir + log_message.table_name + ".xml";
		
		if(log_message.operation == OperationType.CREATE_TABLE){
			createTable(file_path);
		}else if(log_message.operation == OperationType.DROP_TABLE){
			dropTable(file_path);
		}else if(log_message.operation == OperationType.DELETE){
			delete(file_path, log_message.key);
		}else if(log_message.operation == OperationType.STORE){
			store(file_path, log_message.key, log_message.value);
		}
		
		return result;
	}
	
	
	private LogResult forwardRequest(LogMessage log_message){
		LogResult result = null;
		Socket socket = null;
		PrintWriter out = null;
        BufferedReader in = null;
		
        try {   
        	String address = log_message.replicas[log_message.count];
        	String ip = address.split(":")[0];
        	int port = Integer.parseInt(address.split(":")[1]);
        	logger.info("Forwarding request to {}:{}", ip, port);
            socket = new Socket(ip, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            logger.info("Sending..");
            out.print(MessageType.LOG_OPERATION + "\n");
            JAXBContext jaxb_context = JAXBContext.newInstance(LogMessage.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.marshal( log_message, out );
			socket.shutdownOutput(); //To send EOF
			
			logger.info("Waiting for response");
            jaxb_context = JAXBContext.newInstance(LogResult.class);
            result = (LogResult) jaxb_context.createUnmarshaller().unmarshal(in);	
            logger.info("Response received");
            
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
	
	
	private void createTable(String file_path){
		logger.info("Logging create table operation");
		XMLUtility.createFile(file_path);
	}
	
	
	private void dropTable(String file_path){
		logger.info("Logging drop table operation");
		XMLUtility.deleteFile(file_path);
	}
	
	private void delete(String file_path, String key){
		logger.info("Logging delete table operation");
		XMLUtility.addDeleteOperation(file_path, key);
	}
	
	private void store(String file_path, String key, String value){
		logger.info("Logging store table operation");
		XMLUtility.addStoreOperation(file_path, key, value);
	}

}
