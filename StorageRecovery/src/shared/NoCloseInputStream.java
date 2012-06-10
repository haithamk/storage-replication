package shared;

import java.io.FilterInputStream;
import java.io.InputStream;


/**
 * A wrapper class for the input stream. Useful for avoiding implict close of the
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
