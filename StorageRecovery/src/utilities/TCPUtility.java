package utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//OP: we are cool
/**
 * Useful TCP methods.
 * 	http://www.dreamincode.net/forums/topic/139154-transfer-a-file-over-tcp/
 */
public class TCPUtility {


	//=========================================================================
	//================		Members of the class				===============
	//=========================================================================
		
	static final Logger logger = LoggerFactory.getLogger(TCPUtility.class);
	
	
	//=========================================================================
	//====================		Public Methods				===================
	//=========================================================================
	
	/**
	 * Receives a path to a file and a socket connection. Reads the file contents
	 * and send sends them over the socket
	 */
	public static boolean sendFile(String file_path, Socket socket){
		boolean result = false;
		
		try {
			String data = readFile(file_path);
			
			logger.info("Sending file {} over socket", file_path);
			DataOutputStream output = new DataOutputStream(socket.getOutputStream());

			// Step 1 send length
			logger.info("File length is {}", data.length());
			output.writeInt(data.length());
			// Step 2 send length
			output.writeBytes(data); // UTF is a string encoding
			logger.info("File sent successfully");
		}  catch (IOException e) {
			logger.error("Error in IO while sending the file", e);
		} 
		
		return result;
	}
	
	
	/**
	 * Receives a socket connection and a path to a file. Reads the file contents
	 * from the socket and writes it to a file in the specified path
	 */
	public static boolean receiveFile(String file_path, Socket socket){
		boolean result = false;

		try {
			logger.info("Reading file {} from the socket", file_path);
			DataInputStream input = new DataInputStream(socket.getInputStream());
			// Step 1 read length
			int nb = input.readInt();
			logger.info("File length is {}", nb);
			byte[] digit = new byte[nb];
			// Step 2 read byte
			for (int i = 0; i < nb; i++)
				digit[i] = input.readByte();

			String st = new String(digit);
			writeFile(file_path, st);
			logger.info("receicing filing completed successfully");
		}  catch (IOException e) {
			logger.error("Error in IO while receiving the file", e);
		}
		
		return result;
	}

	//=========================================================================
	//====================		Private  Methods			===================
	//=========================================================================
	
	/**
	 * Reads the file contents from disk and returns them as a string
	 */
	private static String readFile(String file_path) throws IOException{
		logger.info("Reading the contents of the file {}", file_path);
		BufferedReader reader = new BufferedReader( new FileReader (file_path));
	    String         line = null;
	    StringBuilder  stringBuilder = new StringBuilder();
	    String         ls = System.getProperty("line.separator");

	    while( ( line = reader.readLine() ) != null ) {
	        stringBuilder.append( line );
	        stringBuilder.append( ls );
	    }
	    reader.close();

	    return stringBuilder.toString();
	}
	
	
	/**
	 * Writes the received contents to a new file in the given path
	 */
	private static void writeFile(String file_path, String content) throws IOException{
		logger.info("Writing file to the disk");
		FileWriter out = new FileWriter(file_path);
		BufferedWriter bufWriter = new BufferedWriter(out);
		bufWriter.append(content);
		bufWriter.close();
		logger.info("Writing the file completed successfully");
	}
	
}
