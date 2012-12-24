package messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class DataNodesAddresses extends Message {

	@XmlElement
	public String[] addresses;
}
