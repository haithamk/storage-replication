package partitionManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import messages.Message.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


import partitionManager.Recovery.TableInfo;
import utilities.TCPFileUtility;
import utilities.XMLUtility;

public class TableBuilder implements Runnable {

	TableInfo table_info;
	PartitionManagerDB pm_db;
	static final Logger logger = LoggerFactory.getLogger(TableBuilder.class);
	
	public TableBuilder(PartitionManagerDB pm_db, TableInfo table_info){
		this.pm_db = pm_db;
		this.table_info = table_info;
	}
	
	@Override
	public void run() {
		//TODO 1. retrieve XML from data Node
		//2. construct table
		//3. save to db
		
		Document doc = getRecoveryXML(table_info.table_name, table_info.dn_ip, table_info.dn_port);
		Table table = buildTable(table_info.table_name, doc);
		pm_db.addTable(table_info.table_name, table);
		
	}
	
	
	private Document getRecoveryXML(String table_name, String dn_ip, int dn_port){
		Document doc = null;
		Socket socket = null;
		PrintWriter out = null;
        BufferedReader in = null;
        
        try {   
            socket = new Socket(dn_ip, dn_port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            out.print(MessageType.RECOVER + "\n");
            out.print(table_name);
            socket.shutdownOutput(); //To send EOF. TODO Do we need this ?
            
            File f = File.createTempFile("REC", "xml");
            TCPFileUtility.receiveFile(f.getPath(), socket);
            out.close();
            in.close();
            socket.close();
            
            return XMLUtility.getDocument(f.getPath());
        }  catch (UnknownHostException e) {
			logger.error("Error communicating with the remote node", e);
		} catch (IOException e) {
			logger.error("Error communicating with the remote node", e);
		}
		
		return doc;
	}
	
	
	private Table buildTable(String table_name, Document doc){
		Table table = new Table(table_name);
		
		NodeList node_list = doc.getElementsByTagName("Entry");

		for(int i = 0; i < node_list.getLength(); i++){
			Element elem = (Element) node_list.item(i);
			String operation = elem.getAttribute("Type");
			String key = elem.getAttribute("Key");
			if(operation.equals("STORE")){
				String value = elem.getTextContent();
				table.store(key, value);
			}else if(operation.equals("DELETE")){
				table.delete(key);
			}else{
				logger.error("Unkown operation type!");
			}
		}
		
		return table;
	}

}
