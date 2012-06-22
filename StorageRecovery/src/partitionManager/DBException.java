package partitionManager;

public class DBException extends Exception{
	
	public enum Cause{
		TABLE_ALREADY_EXIST,
		TABLE_DOESNT_EXIST
	}
	
	Cause cause;
	
	public DBException(Cause cause){
		super();
		this.cause = cause;
	}
}
