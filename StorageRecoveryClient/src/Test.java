import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import javax.xml.bind.JAXBContext;


import messages.PMAddressMsg;
import messages.Message.MessageType;


public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String orch_ip = "127.0.0.1";
		int orch_port = 43201;
		
		Socket socket = null;
		OutputStream out = null;
		try{
			socket = new Socket(orch_ip, orch_port);
			socket.setSoTimeout(60000);
			out = socket.getOutputStream();
			
			
			 PrintWriter pw = new PrintWriter(out, true);
			 pw.println("GET_PM_ADDRESS");

			
//			PMAddressMsg msg = new PMAddressMsg();
//			msg.sender = "Client";
//			msg.type = MessageType.GET_PM_ADDRESS;
			
			JAXBContext jaxb_context = JAXBContext.newInstance(PMAddressMsg.class);
			
//			Marshaller m = jaxb_context.createMarshaller();
//			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);	
//			m.marshal( msg, out );
			socket.shutdownOutput();
			
			BufferedReader inputReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			MessageType type = MessageType.valueOf(inputReader.readLine());
			if(type != MessageType.PM_ADDRESS){
				System.err.println("BAD REPLY");
			}else{
				PMAddressMsg reply = (PMAddressMsg) jaxb_context.createUnmarshaller().unmarshal(socket.getInputStream());
				System.out.println(reply.msg_content);
			}
				
			

		
			socket.close();
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
