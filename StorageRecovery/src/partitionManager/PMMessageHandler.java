package partitionManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import messages.ClientOPMsg;
import messages.ClientOPResult;
import messages.PMAddressMsg;
import messages.ClientOPResult.Status;
import messages.Message.MessageType;

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
			
			BufferedReader inputReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			MessageType type = MessageType.valueOf(inputReader.readLine());
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
			InputStreamReader in = new InputStreamReader(socket.getInputStream());
			JAXBContext jaxb_context = JAXBContext.newInstance(ClientOPMsg.class);
			ClientOPMsg msg = (ClientOPMsg) jaxb_context.createUnmarshaller().unmarshal(in);
			
			//TODO check if the check sum is fine
			//TODO check if the user is authorized
			
			String value = "";
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
				value = pm_db.read(msg.table_name, msg.key);
				break;
			case DELETE:
				pm_db.delete(msg.table_name, msg.key);
				break;
			
			default:
				System.err.println("Partition Manager recieved unknown message type");
				break;
			}
			
			
			ClientOPResult result = new ClientOPResult();
			result.status = Status.SUCCESS;
			result.vlaue = value;
			
			jaxb_context = JAXBContext.newInstance(PMAddressMsg.class);
			
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			OutputStream out = socket.getOutputStream();
			PrintWriter pw = new PrintWriter(out, true);
			 pw.println("PM_ADDRESS");
			 
			m.marshal( result, out );
			out.flush();
			out.close();
			socket.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
