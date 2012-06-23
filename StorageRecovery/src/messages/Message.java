package messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

public abstract class Message {
	
	public static enum MessageType{
		GET_PM_ADDRESS,
		PM_ADDRESS,
		CLIENT_OPERATION,
		CLIENT_OP_RESULT
	};
	
	
	public Message(){
		time = System.currentTimeMillis();
		source = "";
		dest = "";
	}

	@XmlElement
	public String source;
	
	@XmlElement
	public String dest;
	
	@XmlElement
	public long time;
	
	
	public String getHeaders(){
		return String.format("Source: %s\nDest: %s\nTime: %s" , source, dest, time);		
	}
}
