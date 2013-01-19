package messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class RecoverTableMessage extends Message {

	@XmlElement
	public String table_name;
	
	@XmlElement
	public String source;
}
