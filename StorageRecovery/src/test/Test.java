package test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Test {
	
	public static void main(String[] args) throws UnknownHostException {
		InetAddress IPAddress = InetAddress.getByName("193.8.0.19");
		
		System.out.println(IPAddress.toString());
	}
}
