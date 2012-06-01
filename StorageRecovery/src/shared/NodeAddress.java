package shared;


/**
 * This class holds the details of a node in the system.
 *
 */
public class NodeAddress {
	
	//=========================================================================
	//====================		Class Parameters			===================
	//=========================================================================
	
	private String name;
	private String ip;
	private int port;
	
	
	//=========================================================================
	//====================		Constructors				===================
	//=========================================================================
	
	public NodeAddress(String name, String ip, int port) {
		super();
		this.name = name;
		this.ip = ip;
		this.port = port;
	}
	
	
	//=========================================================================
	//====================		Getters/Setters				===================
	//=========================================================================
		
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	
	
	
	
	
}
