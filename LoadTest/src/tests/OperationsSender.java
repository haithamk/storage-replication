package tests;

import java.util.concurrent.ConcurrentLinkedQueue;

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
		
		
		long end = System.currentTimeMillis();
		results.add( ((double)(end-start))/num);
	}

}
