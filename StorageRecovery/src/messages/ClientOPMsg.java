package messages;

import javax.xml.bind.annotation.XmlElement;

public class ClientOPMsg extends Message{

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
	public String table_name;
	
	@XmlElement
	public OperationType type;
	
	@XmlElement
	public String key;
	
	@XmlElement
	public String value;
	
	@XmlElement
	public String check_sum;
	
	
	public String toString(){
		return String.format("user name: %s\npassword: %s\ntable name: %s\ntype:%s\n key: %s\nvalue: %s\n ",
				user_name, password, table_name, type, key, value);
	}
}
