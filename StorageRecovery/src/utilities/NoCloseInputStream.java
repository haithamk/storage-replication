package utilities;

import java.io.FilterInputStream;
import java.io.InputStream;

//OP: not sure if this class is needed
/**
 * A wrapper class for the input stream. Useful for avoiding implicit close of the
 * input stream by class such as JAXB Unmarshaller 
 */
public class NoCloseInputStream extends FilterInputStream {

	public NoCloseInputStream(InputStream in) {
		super(in);
	}
	
	@Override 
	public void close() {
	    // do nothing
	}

}
