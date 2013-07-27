package messages;

import java.util.LinkedList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class RecoverDNMessage extends Message {

	public RecoverDNMessage(){
		
	}
	
	
	public RecoverDNMessage(String dead_node){
		table_names = new LinkedList<String>();
		this.dead_node = dead_node;
		new_nodes = new LinkedList<String>();
	}
	
	@XmlElement
	public String dead_node;
	
	@XmlElement
	public LinkedList<String> table_names;
	
	@XmlElement
	public LinkedList<String> new_nodes;
}
