package messages;

import javax.xml.bind.annotation.XmlElement;

public class ClientOPResult extends Message{
	public enum Status{
		SUCCESS,
		FAIL,
		NOT_AUTHORIZED
	}
	
	@XmlElement
	public Status status;
	
	@XmlElement
	public String vlaue;
}
