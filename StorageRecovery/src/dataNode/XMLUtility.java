package dataNode;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class XMLUtility {

	public static void createFile(String file_path){
		 
		try {
			DocumentBuilderFactory doc_Factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder doc_builder = doc_Factory.newDocumentBuilder();
	 
			// root elements
			Document doc = doc_builder.newDocument();
			Element root_element = doc.createElement("Log");
			doc.appendChild(root_element);
			
			
			saveFile(file_path, doc);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
         
	}
	
	
	
	public static void deleteFile(String file_path){
		File f = new File(file_path);
		f.delete();
	}
	
	public static void addStoreOperation(String file_path, String key, String value){
		addEntry(file_path, "STORE", key, value);
	}
	
	public static void addDeleteOperation(String file_path, String key){
		addEntry(file_path, "DELETE", key, null);
	}
	
	
	private static void addEntry(String file_path, String operation, String key, String value){
		
		try {
			DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance(); 
			domFactory.setIgnoringComments(true);
			DocumentBuilder builder;
			builder = domFactory.newDocumentBuilder();
			Document doc = builder.parse(new File(file_path));
			
			
			Element entry_elem = doc.createElement("Entry");
			entry_elem.setAttribute("Type", operation);
			entry_elem.setAttribute("Key", key);
			if(operation.equals("STORE")){
				entry_elem.appendChild(doc.createTextNode(value));
			}
			
			Element log_element = (Element) doc.getElementsByTagName("Log").item(0);
			log_element.appendChild(entry_elem);
			
			saveFile(file_path, doc);
		
			 
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
	}
	
	
	
	private static void saveFile(String file_path, Document doc){
		try {
			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer;
			transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(file_path));
	 
			// Output to console for testing
			//StreamResult result = new StreamResult(System.out);
	 
			transformer.transform(source, result);
		} catch (TransformerConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
