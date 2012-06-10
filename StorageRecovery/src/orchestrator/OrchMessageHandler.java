package orchestrator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import shared.NoCloseInputStream;
import messages.PMAddressMsg;


/**
 * Handles messages sent to the orchestrator
 * @author Haitham
 *
 */
public class OrchMessageHandler implements Runnable {
	
	Socket socket;				//The received socket
	OrchestratorDB orch_db;		//The data base of the Orchestrator
	JAXBContext jaxb_context;	//JAXB object
	
	public OrchMessageHandler(Socket socket, OrchestratorDB orch_db ){
		this.socket = socket;
		this.orch_db = orch_db;
	}

	@Override
	public void run() {
		try {
			NoCloseInputStream in = new NoCloseInputStream(socket.getInputStream());
			//InputStreamReader in = new InputStreamReader(socket.getInputStream());
			jaxb_context = JAXBContext.newInstance(PMAddressMsg.class);		
			
		//	DebugUtility.printSocket(socket);
			
			
			PMAddressMsg msg = (PMAddressMsg) jaxb_context.createUnmarshaller().unmarshal(in);
			
			System.out.println("Recieved a message from: " + msg.sender);
			switch (msg.type) {
			case GET_PM_ADDRESS:
				handleGetPMAddress();
				break;
			default:
				System.err.println("Orchestrator recieved unknown message type");
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}
	
	
	/**
	 * Handler for GET_PM_ADDRESS Messages. Returns the current PM address
	 * to whom requested it
	 */
	public void handleGetPMAddress(){
		try {
			PMAddressMsg reply = new PMAddressMsg();
			String current_pm = orch_db.getActivePM();
			
			reply.type = PMAddressMsg.MessageType.PM_ADDRESS;
			reply.sender = "Orchestrator";
			reply.msg_content = current_pm;
			
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			OutputStream out = socket.getOutputStream();
			m.marshal( reply, out );
			out.flush();
			out.close();
			socket.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
