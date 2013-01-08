package utilities;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class TCPFileUtility {

	
	//TODO check this class
	
	
	public boolean sendFile(String file_path, Socket socket){
		boolean result = false;
		
		try {
			String data = "Hello, How are you?";
			
			DataOutputStream output = new DataOutputStream(socket.getOutputStream());

			// Step 1 send length
			System.out.println("Length" + data.length());
			output.writeInt(data.length());
			// Step 2 send length
			System.out.println("Writing.......");
			output.writeBytes(data); // UTF is a string encoding
		} catch (UnknownHostException e) {
			System.out.println("Sock:" + e.getMessage());
		} catch (EOFException e) {
			System.out.println("EOF:" + e.getMessage());
		} catch (IOException e) {
			System.out.println("IO:" + e.getMessage());
		} finally {
			if (socket != null)
				try {
					socket.close();
				} catch (IOException e) {/* close failed */
				}
		}
		
		return result;
	}
	
	
	public boolean receiveFile(String file_path, Socket socket){
		boolean result = false;

		try {

			DataInputStream input = new DataInputStream(socket.getInputStream());
			// Step 1 read length
			int nb = input.readInt();
			System.out.println("Read Length" + nb);
			byte[] digit = new byte[nb];
			// Step 2 read byte
			System.out.println("Writing.......");
			for (int i = 0; i < nb; i++)
				digit[i] = input.readByte();

			String st = new String(digit);
			writeFile(file_path, st);
		} catch (EOFException e) {
			System.out.println("EOF:" + e.getMessage());
		} catch (IOException e) {
			System.out.println("IO:" + e.getMessage());
		}

		finally {
			try {
				socket.close();
			} catch (IOException e) {/* close failed */
			}
		}
		
		return result;
	}
	
	
	
	
	
	
	
	private String readFile(String file_path){
		//TODO implement
		return "";
	}
	
	private void writeFile(String file_path, String content) throws IOException{
		FileWriter out = new FileWriter(file_path);
		BufferedWriter bufWriter = new BufferedWriter(out);
		bufWriter.append(content);
		bufWriter.close();
	}
	
}
