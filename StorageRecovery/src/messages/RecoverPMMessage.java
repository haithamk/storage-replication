package messages;

import java.util.LinkedList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class RecoverPMMessage extends Message {

	public RecoverPMMessage(){
		table_names = new LinkedList<String>();
		replicas = new LinkedList<String[]>();
	}
	
	@XmlElement
	public LinkedList<String> table_names;
	
	@XmlElement
	public LinkedList<String[]> replicas;
}
