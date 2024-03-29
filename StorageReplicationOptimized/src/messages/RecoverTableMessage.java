package messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class RecoverTableMessage extends Message {

	public RecoverTableMessage(){}
	
	
	public RecoverTableMessage(String table_name, String source){
		this.table_name = table_name;
		this.reference_table = source;
	}
	
	@XmlElement
	public String table_name;
	
	@XmlElement
	public String reference_table;
}
