package dataNode;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import messages.LogMessage;
import messages.LogMessage.OperationType;
import messages.LogResult;
import messages.LogResult.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utilities.NoCloseInputStream;

public class DNMessageHandler implements Runnable {

	static final Logger logger = LoggerFactory.getLogger(DNMessageHandler.class);
	Socket socket;
	DataNodeDB dn_db;
	
	
	public DNMessageHandler(Socket socket, DataNodeDB dn_db){
		logger.debug("New DNMessageHandler created");
		this.socket = socket;
		this.dn_db = dn_db;
	}
	
	
	@Override
	public void run() {
		try{
			NoCloseInputStream in = new NoCloseInputStream(socket.getInputStream());
			
			LogResult result = new LogResult();
			result.status = Status.SUCCESS;
			
			//Creating marshaler
			 JAXBContext jaxb_context = JAXBContext.newInstance(LogResult.class);			
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
	
	
	
	
	private LogResult logOperation(LogMessage log_message){
		LogResult result = new LogResult();
		
		if( (++log_message.count) < log_message.replicas.length){
			//TODO forward the request to the next replica
		}
		
		String file_path = dn_db.work_dir + log_message.table_name + ".xml";
		
		if(log_message.operation == OperationType.CREATE_TABLE){
			createTable(file_path);
		}else if(log_message.operation == OperationType.DROP_TABLE){
			
		}else if(log_message.operation == OperationType.DELETE){
			
		}else if(log_message.operation == OperationType.STORE){
			
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
