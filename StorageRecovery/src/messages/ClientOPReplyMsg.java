package messages;

import javax.xml.bind.annotation.XmlElement;

public class ClientOPReplyMsg {
	public enum Result{
		SUCCESS,
		FAIL,
		NOT_AUTHORIZED
	}
	
	@XmlElement
	Result result;
	
	@XmlElement
	public String vlaue;
}
