package test;

import java.io.File;
import java.io.OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import messages.PMAddressMsg;



public class MarshlingTest {

	@XmlRootElement
	private static class Msg{
		@XmlElement
		String x = "Msg";
	}
	
	@XmlRootElement
	private static class PMMsg extends Msg{
		@XmlElement
		String y = "PMMsg";
	}
	public static void main(String[] args) {
		testUnmarshal();
		
		
	}
	
	
	public static void testUnmarshal(){
		try{
			JAXBContext jaxb_context = JAXBContext.newInstance(PMMsg.class);
			Msg msg = (Msg) jaxb_context.createUnmarshaller().unmarshal(new File("F:/jaxb_test.txt"));
			System.out.println(msg.x);
			PMMsg pm_msg = (PMMsg) msg;
			System.out.println(pm_msg.y);
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void testMarshal(){
		try{
			
			Msg msg = new Msg();
			PMMsg pm_msg = new PMMsg();
			
			JAXBContext jaxb_context = JAXBContext.newInstance(PMMsg.class);
			Marshaller m = jaxb_context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			m.marshal( msg, System.out);
			m.marshal( pm_msg, System.out);
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
