package debug;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class DebugUtility {

	public static void printSocket(Socket socket){
		try {
		    BufferedReader rd = new BufferedReader(new InputStreamReader(socket.getInputStream()));

		    String str;
		    while ((str = rd.readLine()) != null) {
		        System.out.println(str);
		    }
		    rd.close();
		    
		    
//			StringWriter writer = new StringWriter();
//			IOUtils.copy(in, writer);
//			String theString = writer.toString();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
