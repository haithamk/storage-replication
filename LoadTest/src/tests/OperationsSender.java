package tests;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.restlet.Client;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;

public class OperationsSender implements Runnable {

	ConcurrentLinkedQueue<String> requests;
	ConcurrentLinkedQueue<Double> results;
	
	public OperationsSender(ConcurrentLinkedQueue<String> _requests, ConcurrentLinkedQueue<Double> _results){
		requests = _requests;		
		results = _results;
	}
	
	
	@Override
	public void run() {
		int num = 0;
		long start = System.currentTimeMillis();
		
		while(true){
			String http_url = requests.poll();
			if(http_url == null){
				break;
			}
			
			Client client = new Client(new Context(), Protocol.HTTP);
			ClientResource client_resource = new ClientResource(http_url);
			client_resource.setNext(client);
			Representation rep = client_resource.get();
			//rep.getText();
		}
				
		long end = System.currentTimeMillis();
		results.add( ((double)(end-start))/num);
	}

}
