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
		
		
		table_names.add("1");
		table_names.add("2");
		table_names.add("3");
		
		new_nodes.add("A");
		new_nodes.add("B");
		new_nodes.add("C");
		
	}
	
	@XmlElement
	public String dead_node;
	
	@XmlElement
	public LinkedList<String> table_names;
	
	@XmlElement
	public LinkedList<String> new_nodes;
}
