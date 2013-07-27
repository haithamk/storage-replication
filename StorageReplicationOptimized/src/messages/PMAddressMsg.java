package messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PMAddressMsg extends Message{
	
	@XmlElement
	public String msg_content;	
	
}
