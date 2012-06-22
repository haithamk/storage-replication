package messages;

import javax.xml.bind.annotation.XmlElement;

public class ClientOPResult extends Message{
	public enum ClientOPStatus{
		SUCCESS,
		FAIL,
		NOT_AUTHORIZED,
		TABLE_DOESNT_EXIST
	}
	
	public ClientOPResult(){
		status = ClientOPStatus.SUCCESS;
	}
	
	@XmlElement
	public ClientOPStatus status;
	
	@XmlElement
	public String vlaue;
}
