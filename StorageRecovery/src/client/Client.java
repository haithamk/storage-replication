package client;

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
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import messages.Message;
import messages.PMAddressMsg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import debug.DebugUtility;

import partitionManager.PartitionManager;

public class Client {
	
	static final Logger logger = LoggerFactory.getLogger(Client.class);
	String orch_ip = null;
	int orch_port = -1;

	public Client(String node_id, String config_file){
		logger.info("Starting Client with parameters node_id= " + node_id + " config_file= " + config_file);
		try {
			initConfig(config_file);
		} catch (Exception e) {
			e.printStackTrace();
		}
		printManual();
		
		Scanner scanner = new Scanner( System.in );
		while(true){
			String str = scanner.nextLine();
			if(str.equals("exist")){
				break;
			}else{
				executeCommand(str);
			}
		}
	}
	
	
	
	
	
	private boolean executeCommand(String str){
		String[] cmds = str.split(" ");
		
		if(cmds[0].equals("GETPM")){
			getPM();
			return true;
		}
		return false;
	}
	
	
	
	private void getPM(){
		Socket socket = null;
        PrintWriter out = null;
        BufferedReader in = null;
 
        try {
            socket = new Socket(orch_ip, orch_port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //out.print(Message.MessageType.GET_PM_ADDRESS);
            out.print("GET_PM_ADDRESS" + System.getProperty("line.separator"));
            out.flush();
            
            //DebugUtility.printSocket(socket);
            JAXBContext jaxb_context = JAXBContext.newInstance(PMAddressMsg.class);
            PMAddressMsg msg = (PMAddressMsg) jaxb_context.createUnmarshaller().unmarshal(in);	
            System.out.println("Active PM Address: " + msg.msg_content);

            out.close();
            in.close();
            socket.close();
        } catch (UnknownHostException e) {
            logger.error("Couldn't connect to the Orchestrator");
            return;
        } catch (IOException e) {
        	logger.error("Couldn't get I/O for the connection");
        	return;
        } catch (JAXBException e) {
			logger.error("Error unmarshling reply");
			e.printStackTrace();
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
		orch_ip = xpath.compile("//Orchestrator/ip").evaluate(doc);
		//Loading Orchestrator port
		String dummy_str = xpath.compile("//Orchestrator/port").evaluate(doc);
		orch_port = Integer.parseInt(dummy_str);
		logger.info("orch_ip = " + orch_ip + " orch_port = " + orch_port);

	}
}
