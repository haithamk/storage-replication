package orchestrator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import messages.PMAddressMsg;
import messages.Message.MessageType;


/**
 * Handles messages sent to the orchestrator
 * @author Haitham
 *
 */
public class OrchMessageHandler implements Runnable {
	
	Socket socket;				//The received socket
	OrchestratorDB orch_db;		//The data base of the Orchestrator
	
	public OrchMessageHandler(Socket socket, OrchestratorDB orch_db ){
		this.socket = socket;
		this.orch_db = orch_db;
	}

	@Override
	public void run() {
		try {
			
			BufferedReader inputReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			MessageType type = MessageType.valueOf(inputReader.readLine());

			switch(type){
			case GET_PM_ADDRESS:
				handleGetPMAddress();
				break;
			default:
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
			
			JAXBContext jaxb_context = JAXBContext.newInstance(PMAddressMsg.class);
			
			PMAddressMsg reply = new PMAddressMsg();
			String current_pm = orch_db.getActivePM();
			
			reply.msg_content = current_pm;
			
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			OutputStream out = socket.getOutputStream();
			PrintWriter pw = new PrintWriter(out, true);
			 pw.println("PM_ADDRESS");
			 
			m.marshal( reply, out );
			out.flush();
			out.close();
			socket.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
	}

}
