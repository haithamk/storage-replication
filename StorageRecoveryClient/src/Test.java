import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import messages.PMAddressMsg;
import messages.PMAddressMsg.MessageType;


public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String orch_ip = "127.0.0.1";
		int orch_port = 43201;
		
		Socket socket = null;
		ObjectOutputStream out = null;
		try{
			socket = new Socket(orch_ip, orch_port);
			socket.setSoTimeout(1000);
			out = new ObjectOutputStream(socket.getOutputStream());
			
			
			PMAddressMsg msg = new PMAddressMsg();
			msg.sender = "Client";
			msg.type = MessageType.GET_PM_ADDRESS;
			
			JAXBContext jaxb_context = JAXBContext.newInstance(PMAddressMsg.class);
			
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			m.marshal( msg, out );
			
			out.flush();
		}catch(Exception e){
			e.printStackTrace();
		}finally
		{
			try
			{
				if(out != null)
					out.close();
				if(socket != null)
					socket.close();
			}
			catch(IOException e){}
		}
		

	}

}
