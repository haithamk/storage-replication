package dataNode;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

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

}
