package partitionManager;

public class DBException extends Exception{
	
	private static final long serialVersionUID = 6057229956400161243L;

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
