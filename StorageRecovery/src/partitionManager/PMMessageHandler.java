package partitionManager;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utilities.NoCloseInputStream;

import messages.ClientOPMsg;
import messages.ClientOPResult;
import messages.ClientOPResult.ClientOPStatus;
import messages.Message.MessageType;

public class PMMessageHandler implements Runnable {

	static final Logger logger = LoggerFactory.getLogger(PMMessageHandler.class);
	Socket socket;
	PartitionManagerDB pm_db;
	
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
				executeClientOperation();
				break;
			default:
				break;					
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	public void executeClientOperation(){
		try{
			NoCloseInputStream in = new NoCloseInputStream(socket.getInputStream());
			JAXBContext jaxb_context = JAXBContext.newInstance(ClientOPMsg.class);
			ClientOPMsg msg = (ClientOPMsg) jaxb_context.createUnmarshaller().unmarshal(in);			
			logger.debug("Message headers:\n{}\nMessage content:\n{}", msg.getHeaders(), msg.toString());
			
			//TODO check if the check sum is fine
			//TODO check if the user is authorized
			
			
			ClientOPResult result = new ClientOPResult();			
			try{				
				switch (msg.type){
				case CREATE_TABLE:
					pm_db.createTable(msg.table_name);
					break;
				case DROP_TABLE:
					pm_db.dropTable(msg.table_name);
					break;
				case STORE:
					pm_db.store(msg.table_name, msg.key, msg.value);
					break;
				case READ:
					result.vlaue = pm_db.read(msg.table_name, msg.key);
					break;
				case DELETE:
					pm_db.delete(msg.table_name, msg.key);
					break;
				
				default:
					logger.warn("Partition manager recieved unknown message type: {}", msg.type);
					break;
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
			
			
			//Creating marshaler
			jaxb_context = JAXBContext.newInstance(ClientOPResult.class);			
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			//Sending the result
			OutputStream out = socket.getOutputStream();
			PrintWriter pw = new PrintWriter(out, true);
			pw.println(MessageType.CLIENT_OP_RESULT);
			 
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

}
