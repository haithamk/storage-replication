package messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class LogResult extends Message {
	
	@XmlEnum(String.class)
	public enum Status{
		SUCCESS,
		NO_DN_AVAILABLE
	}

	@XmlElement
	public Status status;

}
