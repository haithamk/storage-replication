import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;


import messages.*;
import messages.ClientOPMsg.OperationType;
import messages.Message.MessageType;


public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String pm_address = getActivePMAdress();
		System.out.println("Partition Manager address: " + pm_address);
		String pm_ip = pm_address.split(":")[0];
		int pm_port = Integer.parseInt(pm_address.split(":")[1]);
		
		String table_name = "MyTable";
		createTable(pm_ip, pm_port, table_name);
		store(pm_ip, pm_port, table_name, "Name", "Haitham");
		store(pm_ip, pm_port, table_name, "Name", "Muhammed");
		read(pm_ip, pm_port, table_name, "Name");
	}
	
	
	static String getActivePMAdress(){
		String orch_ip = "127.0.0.1";
		int orch_port = 43000;
		
		Socket socket = null;
		OutputStream out = null;
		try{
			socket = new Socket(orch_ip, orch_port);
			socket.setSoTimeout(60000);
			
			
			//Sending request
			out = socket.getOutputStream();			
			PrintWriter pw = new PrintWriter(out, true);
			pw.println(MessageType.GET_PM_ADDRESS);
			socket.shutdownOutput();
			 

			//Getting reply type
			DataInputStream  inputReader = new DataInputStream ((socket.getInputStream()));
			@SuppressWarnings("deprecation")
			MessageType type = MessageType.valueOf(inputReader.readLine());
			
			if(type != MessageType.PM_ADDRESS){
				System.err.println("BAD REPLY");
			}else{
				//Reading result
				JAXBContext jaxb_context = JAXBContext.newInstance(PMAddressMsg.class);
				PMAddressMsg reply = (PMAddressMsg) jaxb_context.createUnmarshaller().unmarshal(socket.getInputStream());
				return reply.msg_content;
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
		
		
		return null;
	}
	
	
	static void createTable(String pm_ip, int pm_port, String table_name){
		
		
		
		Socket socket = null;
		OutputStream out = null;
		try{
			socket = new Socket(pm_ip, pm_port);
			socket.setSoTimeout(60000);
			
			
			//Sending request
			ClientOPMsg msg = new ClientOPMsg();
			msg.table_name = table_name;
			msg.type = OperationType.CREATE_TABLE;
			
			out = socket.getOutputStream();			
			PrintWriter pw = new PrintWriter(out, true);
			pw.println(MessageType.CLIENT_OPERATION);
			JAXBContext jaxb_context = JAXBContext.newInstance(ClientOPMsg.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.marshal(msg, System.out);
			m.marshal( msg, out );
			
			socket.shutdownOutput();
			 

			//Getting reply type
			DataInputStream  inputReader = new DataInputStream ((socket.getInputStream()));
			@SuppressWarnings("deprecation")
			MessageType type = MessageType.valueOf(inputReader.readLine());
			
			if(type != MessageType.CLIENT_OP_RESULT){
				System.err.println("BAD REPLY: " + type);
			}else{
				//Reading result
				jaxb_context = JAXBContext.newInstance(ClientOPResult.class);
				ClientOPResult reply = (ClientOPResult) jaxb_context.createUnmarshaller().unmarshal(socket.getInputStream());
				System.out.println(reply.status);
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

	
	static void store(String pm_ip, int pm_port, String table_name, String key, String value){
		Socket socket = null;
		OutputStream out = null;
		try{
			socket = new Socket(pm_ip, pm_port);
			socket.setSoTimeout(60000);
			
			
			//Sending request
			ClientOPMsg msg = new ClientOPMsg();
			msg.table_name = table_name;
			msg.type = OperationType.STORE;
			msg.key = key;
			msg.value = value;
			
			out = socket.getOutputStream();			
			PrintWriter pw = new PrintWriter(out, true);
			pw.println(MessageType.CLIENT_OPERATION);
			JAXBContext jaxb_context = JAXBContext.newInstance(ClientOPMsg.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.marshal(msg, System.out);
			m.marshal( msg, out );
			
			socket.shutdownOutput();
			 

			//Getting reply type
			DataInputStream  inputReader = new DataInputStream ((socket.getInputStream()));
			@SuppressWarnings("deprecation")
			MessageType type = MessageType.valueOf(inputReader.readLine());
			
			if(type != MessageType.CLIENT_OP_RESULT){
				System.err.println("BAD REPLY: " + type);
			}else{
				//Reading result
				jaxb_context = JAXBContext.newInstance(ClientOPResult.class);
				ClientOPResult reply = (ClientOPResult) jaxb_context.createUnmarshaller().unmarshal(socket.getInputStream());
				System.out.println(reply.status);
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
	
	
	
	static void read(String pm_ip, int pm_port, String table_name, String key){
		Socket socket = null;
		OutputStream out = null;
		try{
			socket = new Socket(pm_ip, pm_port);
			socket.setSoTimeout(60000);
			
			
			//Sending request
			ClientOPMsg msg = new ClientOPMsg();
			msg.table_name = table_name;
			msg.type = OperationType.READ;
			msg.key = key;
			
			out = socket.getOutputStream();			
			PrintWriter pw = new PrintWriter(out, true);
			pw.println(MessageType.CLIENT_OPERATION);
			JAXBContext jaxb_context = JAXBContext.newInstance(ClientOPMsg.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			m.marshal(msg, System.out);
			m.marshal( msg, out );
			
			socket.shutdownOutput();
			 

			//Getting reply type
			DataInputStream  inputReader = new DataInputStream ((socket.getInputStream()));
			@SuppressWarnings("deprecation")
			MessageType type = MessageType.valueOf(inputReader.readLine());
			
			if(type != MessageType.CLIENT_OP_RESULT){
				System.err.println("BAD REPLY: " + type);
			}else{
				//Reading result
				jaxb_context = JAXBContext.newInstance(ClientOPResult.class);
				ClientOPResult reply = (ClientOPResult) jaxb_context.createUnmarshaller().unmarshal(socket.getInputStream());
				System.out.println(reply.status);
				System.out.println("Value read: " + reply.vlaue);
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
