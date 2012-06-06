package orchestrator;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import shared.Message;
import shared.Message.MessageType;

public class MessageHandler implements Runnable {
	
	Socket socket;
	OrchestratorDB orch_db;
	JAXBContext jaxb_context;
	
	public MessageHandler(Socket socket, OrchestratorDB orch_db ){
		this.socket = socket;
		this.orch_db = orch_db;
	}

	@Override
	public void run() {
		try {
			InputStreamReader in = new InputStreamReader(socket.getInputStream());
			jaxb_context = JAXBContext.newInstance(Message.class);
			Message msg = (Message) jaxb_context.createUnmarshaller().unmarshal(in);
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
			Message reply = new Message();
			String current_pm = orch_db.getActivePM();
			
			reply.type = MessageType.PM_ADDRESS;
			reply.sender = "Orchestrator";
			reply.msg_content = current_pm;
			
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			OutputStream out = socket.getOutputStream();
			m.marshal( reply, out );
			out.flush();
			out.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
