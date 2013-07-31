package utilities;

import java.io.FilterOutputStream;
import java.io.OutputStream;

//OP: not sure if this class is needed at all
public class NoCloseOutputStream extends FilterOutputStream {

	public NoCloseOutputStream(OutputStream out) {
		super(out);
	}
	
	@Override 
	public void close() {
	    // do nothing
	}

}
