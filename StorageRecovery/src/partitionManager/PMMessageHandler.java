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
		switch (msg.type){
		case CREATE_TABLE:
			break;
		case DROP_TABLE:
			break;
		case STORE:
			break;
		case READ:
			break;
		case DELETE:
			break;
		
		default:
			System.err.println("Partition Manager recieved unknown message type");
			break;
		}
	}

}
