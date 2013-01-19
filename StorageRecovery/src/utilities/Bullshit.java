package utilities;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import messages.RecoverTableMessage;
import messages.Message.MessageType;

public class Bullshit {

	/**
	 * Reads the XML file of the requested table and transfers it over socket to
	 * the destination
	 */
/*	private void handleRecover(){
		try {
			
			//Init input/output streams
			XMLEventReader xer = XMLInputFactory.newInstance().createXMLEventReader(socket.getInputStream());
			
			
			//Read log message
			JAXBContext jaxb_context = JAXBContext.newInstance(RecoverTableMessage.class);
			RecoverTableMessage recover_msg = (RecoverTableMessage) jaxb_context.createUnmarshaller().unmarshal(xer);
			
			String table_name = recover_msg.table_name;
			String file_path = dn_db.work_dir + table_name + ".xml";
			logger.info("Handling recover request for table: {} ", table_name); 
			
			if(recover_msg.dest.equals("")){
				//Send the given file over the socket
				TCPUtility.sendFile(file_path, socket);
				xer.close();
				socket.close();
			}else{
				socket.close();
				String ip = recover_msg.dest.split(":")[0];
	        	int port = Integer.parseInt(recover_msg.dest.split(":")[1]);
	        	Socket socket2 = new Socket(ip, port);
	        	PrintWriter out = new PrintWriter(new NoCloseOutputStream(socket.getOutputStream()), true);
	        	out.println(MessageType.TABLE_XML);
	        	out.flush();
	        	TCPUtility.sendFile(file_path, socket2);
	        	out.close();
	        	socket2.close();
			}
			
			logger.info("Handling request completed successfully");	
		} catch (JAXBException e) {
			logger.error("Error marshling/unmarshling", e);
		} catch (IOException e) {
			logger.error("IO Error", e);
		} catch (XMLStreamException e) {
			logger.error("Error in the XML reader/writer", e);
		}			
	}*/
}
