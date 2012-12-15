package messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClientOPMsg extends Message{

	@XmlEnum(String.class)
	public enum OperationType{
		CREATE_TABLE,
		DROP_TABLE,
		STORE,
		READ,
		DELETE,
	}
	
	public ClientOPMsg(){
		user_name = "";
		password = "";
		table_name = "";
		key = "";
		value = "";
		check_sum = "";
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
		return String.format("user name: %s\npassword: %s\ntable name: %s\ntype:%s\nkey: %s\nvalue: %s\n ",
				user_name, password, table_name, type, key, value);
	}
}
