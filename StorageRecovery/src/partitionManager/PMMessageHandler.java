package partitionManager;

import java.io.InputStreamReader;
import java.net.Socket;

import javax.xml.bind.JAXBContext;

import messages.ClientOPMsg;

public class PMMessageHandler implements Runnable {

	Socket socket;
	PartitionManagerDB pm_db;
	
	public PMMessageHandler(Socket socket, PartitionManagerDB pm_db) {
		this.socket = socket;
		this.pm_db = pm_db;
	}

	@Override
	public void run() {
		try {
			InputStreamReader in = new InputStreamReader(socket.getInputStream());
			JAXBContext jaxb_context = JAXBContext.newInstance(ClientOPMsg.class);
			ClientOPMsg msg = (ClientOPMsg) jaxb_context.createUnmarshaller().unmarshal(in);
			//TODO check if the user is authorized
			
			//TODO check if the check sum is fine
			
			executeOperation(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	public void executeOperation(ClientOPMsg msg){
		String value;
		switch (msg.type){
		case CREATE_TABLE:
			pm_db.createeTable(msg.table_name);
			break;
		case DROP_TABLE:
			pm_db.dropTable(msg.table_name);
			break;
		case STORE:
			pm_db.store(msg.table_name, msg.key, msg.value);
			break;
		case READ:
			value = pm_db.read(msg.table_name, msg.key);
			break;
		case DELETE:
			pm_db.delete(msg.table_name, msg.key);
			break;
		
		default:
			System.err.println("Partition Manager recieved unknown message type");
			break;
		}
	}

}
