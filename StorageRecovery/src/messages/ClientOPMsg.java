package messages;

import javax.xml.bind.annotation.XmlElement;

public class ClientOPMsg {

	public enum OperationType{
		CREATE_TABLE,
		DROP_TABLE,
		STORE,
		READ,
		DELETE,
	}
	
	@XmlElement
	public String user_name;
	
	@XmlElement
	public String password;
	
	@XmlElement
	public String tabel_name;
	
	@XmlElement
	public OperationType type;
	
	@XmlElement
	public String key;
	
	@XmlElement
	public String value;
	
	@XmlElement
	public String check_sum;
}
