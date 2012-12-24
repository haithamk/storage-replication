package dataNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DNMessageHandler implements Runnable {

	static final Logger logger = LoggerFactory.getLogger(DNMessageHandler.class);
	
	public DNMessageHandler(){
		logger.debug("New DNMessageHandler created");
	}
	
	
	@Override
	public void run() {
		try{
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
