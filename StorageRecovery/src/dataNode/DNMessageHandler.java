package dataNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import messages.LogMessage;
import messages.LogMessage.OperationType;
import messages.LogResult;
import messages.LogResult.Status;
import messages.Message.MessageType;
import messages.RecoverTableMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utilities.NoCloseOutputStream;
import utilities.TCPUtility;
import utilities.XMLUtility;

public class DNMessageHandler implements Runnable {

	//=========================================================================
	//================		Members of the class				===============
	//=========================================================================
	
	static final Logger logger = LoggerFactory.getLogger(DNMessageHandler.class);
	
	Socket socket;
	DataNodeDB dn_db;
	BufferedReader inputReader;
	
	
	//=========================================================================
	//====================		Public Methods				===================
	//=========================================================================
	
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
			case NEW_TABLE:
				handleNewTable();
				break;
			default:
				break;
			}
			
		} catch (IOException e) {
			logger.error("IO error occurred while handling message", e);
		}
	}
	
	
	//=========================================================================
	//================			Auxiliary Methods				===============
	//=========================================================================
	
	private void handleNewTable(){
		try {
			
			//Init input/output streams
			XMLEventReader xer = XMLInputFactory.newInstance().createXMLEventReader(socket.getInputStream());
			
			//Read log message
			JAXBContext jaxb_context = JAXBContext.newInstance(RecoverTableMessage.class);
			RecoverTableMessage recover_msg = (RecoverTableMessage) jaxb_context.createUnmarshaller().unmarshal(xer);
			socket.close();
			
			String table_name = recover_msg.table_name;
			String file_path = dn_db.work_dir + table_name + ".xml";
			String reference_table  = recover_msg.reference_table;
			logger.info("Handling recover request for table: {} ", table_name); 
			
			
			String ip = reference_table.split(":")[0];
        	int port = Integer.parseInt(reference_table.split(":")[1]);
        	Socket socket2 = new Socket(ip, port);
        	
        	PrintWriter out = new PrintWriter(socket2.getOutputStream(), true);
        	out.println(MessageType.RECOVER);
            out.println(table_name);
            out.flush();
            
            TCPUtility.receiveFile(file_path, socket2);
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
			TCPUtility.sendFile(file_path, socket);
			
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
			
			System.out.println("1");
			System.out.println("2");
			System.out.println("3");
			
			//Init input/output streams
			XMLEventReader xer = XMLInputFactory.newInstance().createXMLEventReader(socket.getInputStream());
			XMLStreamWriter xsw = XMLOutputFactory.newInstance().createXMLStreamWriter(socket.getOutputStream()); 
			
			//Read log message
			JAXBContext jaxb_context = JAXBContext.newInstance(LogMessage.class);
			LogMessage log_msg = (LogMessage) jaxb_context.createUnmarshaller().unmarshal(xer);
			
			//Log operation in persistent disk
			result = logOperation(log_msg);
			
			//Creating marshaler
			jaxb_context = JAXBContext.newInstance(LogResult.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.setProperty(Marshaller.JAXB_FRAGMENT,true);
			//Sending the result
			m.marshal( result, xsw );
			xsw.flush();
			
			//Close connection
			xer.close();
			xsw.close();
			socket.close();
			logger.info("Handling request completed successfully");	
		} catch (JAXBException e) {
			logger.error("Error marshling/unmarshling", e);
		} catch (IOException e) {
			logger.error("IO Error", e);
		} catch (XMLStreamException e) {
			logger.error("Error in the XML reader/writer", e);
		}			
		
	}
	
	
	private LogResult logOperation(LogMessage log_message){
		
		LogResult result = new LogResult();;
		
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
		
		result.status = Status.SUCCESS;
		return result;
	}
	
	
	private LogResult forwardRequest(LogMessage log_message){
		LogResult result = null;
		Socket socket2 = null;
		PrintWriter out = null;
		
        try {   
        	//Creating socket
        	String address = log_message.replicas[log_message.count];
        	String ip = address.split(":")[0];
        	int port = Integer.parseInt(address.split(":")[1]);
        	logger.info("Forwarding request to {}:{}", ip, port);
        	socket2 = new Socket(ip, port);
        	
        	//Init input/output streams
            out = new PrintWriter(socket2.getOutputStream(), true);
            
            //Sending operation type
            out.println(MessageType.LOG_OPERATION);
            out.flush();
            
            //Sending log message
            XMLStreamWriter xsw = XMLOutputFactory.newInstance().createXMLStreamWriter(socket2.getOutputStream()); 
            JAXBContext jaxb_context = JAXBContext.newInstance(LogMessage.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.setProperty(Marshaller.JAXB_FRAGMENT,true);
			//Sending the result
			m.marshal( log_message, xsw );
			xsw.flush();
			
			//get response
			XMLEventReader xer = XMLInputFactory.newInstance().createXMLEventReader(socket2.getInputStream());
			logger.info("Waiting for response");
            jaxb_context = JAXBContext.newInstance(LogResult.class);
            result = (LogResult) jaxb_context.createUnmarshaller().unmarshal(xer);	
            logger.info("Response received");
            
            //Close connection
			xer.close();
			xsw.close();
            out.close();
            socket2.close();
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
