package shared;

import javax.xml.bind.annotation.XmlElement;

public class Message {

	public enum MessageType{
		GET_PM_ADDRESS,
		PM_ADDRESS
	}
	
	@XmlElement
	public String sender;
	
	@XmlElement
	public MessageType type;
	
	@XmlElement
	public String msg_content;
	
}
