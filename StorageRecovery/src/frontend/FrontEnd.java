package frontend;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import messages.ClientOPMsg;
import messages.ClientOPResult;
import messages.PMAddressMsg;
import messages.ClientOPResult.ClientOPStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import org.restlet.*;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;


public class FrontEnd extends Component{

	//=========================================================================
	//================		Members of the class				===============
	//=========================================================================
		
	static final Logger logger = LoggerFactory.getLogger(FrontEnd.class);
	String orch_ip = null;
	int orch_port = -1;
	String pm_ip = null;
	int pm_port = -1;

	
	//=========================================================================
	//====================		Public Methods				===================
	//=========================================================================
	public FrontEnd(String node_id, String config_file){
		logger.info("Starting Client with parameters node_id= " + node_id + " config_file= " + config_file);
		try {
			initConfig(config_file);
		} catch (Exception e) {
			e.printStackTrace();
		}
		printManual();
	}
	
	
	public void run(){
		Scanner scanner = null;
		try {
			createServer();
			scanner = new Scanner( System.in );
			while(true){
				String str = scanner.nextLine();
				if(str.equals("exit")){
					break;
				}else{
					executeCommand(str);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(scanner != null)
				scanner.close();
		}
	}
	
	//=========================================================================
	//================			Auxiliary Methods				===============
	//=========================================================================
	
	public void createServer(){
		try {
			new Server(Protocol.HTTP, 8182, this).start();
        } catch (Exception e) {
            // Something is wrong.
            e.printStackTrace();
        }
	}
	
	@Override
	public void handle(Request request, Response response){
		super.handle(request, response);
		
		response.setEntity(request.getResourceRef().getRemainingPart(), MediaType.TEXT_PLAIN);  
	}
	
	//=========================================================================
	//================			Auxiliary Methods				===============
	//=========================================================================
	
	private void executeCommand(String str){
		String[] cmds = str.split(" ");
		Operation operation = new Operation();
		
		if(cmds[0].toUpperCase().equals("GETPM")){
			operation.getPM();
		}else if(cmds[0].toUpperCase().equals("CREATE")){
			operation.createTable(cmds[1]);
		}else if(cmds[0].toUpperCase().equals("DROP")){
			operation.dropTable(cmds[1]);
		}else if(cmds[0].toUpperCase().equals("STORE")){
			operation.store(cmds[1], cmds[2], cmds[3]);
		}else if(cmds[0].toUpperCase().equals("READ")){
			operation.read(cmds[1], cmds[2]);
		}else if(cmds[0].toUpperCase().equals("DELETE")){
			operation.delete(cmds[1], cmds[2]);
		}
		
	}
	
	
	
	
	
	
	
private void printManual(){	
	
	
		File sourceFile = new File ("manual.txt");
		FileReader fr = null;
		try {
			fr = new FileReader(sourceFile);
			int inChar;
			System.out.println("");
			System.out.println("");
			while ((inChar = fr.read()) != -1) {
				System.out.printf("%c", inChar);
			}
			System.out.println("");
			System.out.println("");
		} catch (IOException e) {
			logger.error("Failure while reading manual file: %s\n", e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if (fr != null) {
					fr.close();
				}
			} catch (IOException e) {
				logger.error("Error closing file reader: %s\n", e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	
	private void initConfig(String config_file) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		//Loading XML document
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		Document doc = builder.parse(config_file);		
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		
		
		
		//Loading Orchestrator address
		Operation.orch_ip = xpath.compile("//Orchestrator/ip").evaluate(doc);
		//Loading Orchestrator port
		String dummy_str = xpath.compile("//Orchestrator/port").evaluate(doc);
		Operation.orch_port = Integer.parseInt(dummy_str);
		logger.info("orch_ip = " + orch_ip + " orch_port = " + orch_port);

	}
}
