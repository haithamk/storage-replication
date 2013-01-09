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
	
	
	
	
	private void handleRecover(){
		//TODO read the xml file and transfer it over socket to the dest
		try{
			String table_name = inputReader.readLine();
			String file_path = dn_db.work_dir + table_name + ".xml";
			
			TCPFileUtility.sendFile(file_path, socket);
			
			socket.close();
			logger.info("getTableReplicas result sent successfully");
		}catch(Exception e){
			logger.error("An error occurred in the method getTableReplicas", e);
		}
		
	}
	
	
	private void handleLogOperation(){
		try {
			LogResult result = new LogResult();
			result.status = Status.SUCCESS;
			
			NoCloseInputStream in = new NoCloseInputStream(socket.getInputStream());
			JAXBContext jaxb_context = JAXBContext.newInstance(LogMessage.class);
			LogMessage log_msg = (LogMessage) jaxb_context.createUnmarshaller().unmarshal(in);			
			
			
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
			//TODO forward the request to the next replica
			LogResult forward_result = forwardRequest(log_message);
			if(forward_result.status != Status.SUCCESS){
				//TODO handle failure
				return forward_result;
			}
		}
		
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
            socket = new Socket(ip, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            out.print(MessageType.LOG_OPERATION + "\n");
            JAXBContext jaxb_context = JAXBContext.newInstance(LogMessage.class);
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
	
	
	private void createTable(String file_path){
		XMLUtility.createFile(file_path);
	}
	
	
	private void dropTable(String file_path){
		XMLUtility.deleteFile(file_path);
	}
	
	private void delete(String file_path, String key){
		XMLUtility.addDeleteOperation(file_path, key);
	}
	
	private void store(String file_path, String key, String value){
		XMLUtility.addStoreOperation(file_path, key, value);
	}

}
