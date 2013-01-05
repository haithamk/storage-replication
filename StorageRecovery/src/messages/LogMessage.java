package messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class LogMessage extends Message {

	@XmlElement
	public String operation;
	
	@XmlElement
	public String[] replicas;
	
	@XmlElement
	public String table_name;
	
	@XmlElement
	public String key;
	
	@XmlElement
	public String value;
	
}
