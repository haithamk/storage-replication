package orchestrator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collection;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import orchestrator.OrchestratorDB.NodeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import messages.DataNodesAddresses;
import messages.PMAddressMsg;
import messages.Message.MessageType;


/**
 * Handles messages sent to the orchestrator
 * @author Haitham
 *
 */
public class OrchMessageHandler implements Runnable {
	
	static final Logger logger = LoggerFactory.getLogger(OrchMessageHandler.class);
	Socket socket;				//The received socket
	OrchestratorDB orch_db;		//The data base of the Orchestrator
	BufferedReader inputReader;
	
	public OrchMessageHandler(Socket socket, OrchestratorDB orch_db ){
		logger.debug("New OrchMessageHandler created");
		this.socket = socket;
		this.orch_db = orch_db;
	}

	@Override
	public void run() {
		try {
			
			inputReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			MessageType type = MessageType.valueOf(inputReader.readLine());
			logger.info("Handling message of type: {}", type);
			
			switch(type){
			case RESET:
				handleReset();
			case GET_PM_ADDRESS:
				handleGetPMAddress();
				break;
			case GET_TABLE_REPLICAS:
				getTableReplicas();
			default:
				break;
			}
			
	
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}
	
	
	public void handleReset(){
		Collection<NodeInfo> nodes = orch_db.nodes.values();
		for(NodeInfo node: nodes){
			sendResetToNode(node);
		}
		orch_db.reset();
		
	}
	
	private void sendResetToNode(NodeInfo node){
		//TODO
	}
	
	public void getTableReplicas(){
		try{
			String table_name = inputReader.readLine();
			String[] replicas = orch_db.assignTableReplicas(table_name);
			
			DataNodesAddresses addresses = new DataNodesAddresses();
			addresses.addresses = replicas;
					
			JAXBContext jaxb_context = JAXBContext.newInstance(DataNodesAddresses.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			OutputStream out = socket.getOutputStream();
			m.marshal( addresses, out );
			out.flush();
			out.close();
			socket.close();
			logger.info("getTableReplicas result sent successfully");
		}catch(Exception e){
			logger.error("An error occurred in the method getTableReplicas", e);
		}
	}
	
	/**
	 * Handler for GET_PM_ADDRESS Messages. Returns the current PM address
	 * to whom requested it
	 */
	public void handleGetPMAddress(){
		try {
			
			//Creating reply message
			PMAddressMsg reply = new PMAddressMsg();
			String current_pm = orch_db.getActivePM();			
			reply.msg_content = current_pm;
			logger.debug("Reply message with address: {}", current_pm);
			
			//Creating marshller
			JAXBContext jaxb_context = JAXBContext.newInstance(PMAddressMsg.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			//Sending message
			logger.debug("Sending reply");
			OutputStream out = socket.getOutputStream();
			//PrintWriter pw = new PrintWriter(out, true);
			//pw.println("PM_ADDRESS");
			 
			m.marshal( reply, out );
			out.flush();
			out.close();
			//socket.shutdownOutput();
			socket.close();
			logger.info("Handling request completed successfully");
			
		} catch (IOException e) {
			logger.error("IO error occurred while handling message", e);
		} catch (JAXBException e) {
			logger.error("Jaxb error occurred while handling message", e);
		}
	}

}
