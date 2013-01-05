package dataNode;

import java.io.File;

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

public class XMLUtility {

	public static void createFile(String path){
		 
		try {
			DocumentBuilderFactory doc_Factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder doc_builder = doc_Factory.newDocumentBuilder();
	 
			// root elements
			Document doc = doc_builder.newDocument();
			Element root_element = doc.createElement("Entries");
			doc.appendChild(root_element);
			
			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			//StreamResult result = new StreamResult(new File("C:\\file.xml"));
	 
			// Output to console for testing
			StreamResult result = new StreamResult(System.out);
	 
			transformer.transform(source, result);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
         
	}
	
	public static void addEntry(){
		
	}
}
