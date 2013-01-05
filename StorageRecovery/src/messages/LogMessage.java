package messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class LogMessage extends Message {

	public LogMessage(){
		count = 0;
	}
	
	
	@XmlEnum(String.class)
	public enum OperationType{
		CREATE_TABLE,
		DROP_TABLE,
		STORE,
		DELETE,
	}
	
	@XmlElement
	public OperationType operation;
	
	@XmlElement
	public String[] replicas;
	
	@XmlElement
	public String table_name;
	
	@XmlElement
	public String key;
	
	@XmlElement
	public String value;
	
	@XmlElement
	public int count;
	
}
