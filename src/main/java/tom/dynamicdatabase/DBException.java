package tom.dynamicdatabase;

/**
 * The exception class for the dynamic db errors
 * @author Tom Wimberg
 *
 */
public class DBException extends Exception {

	private static final long serialVersionUID = -6131608633425014893L;
	
	public DBException() {}
	
	public DBException(String message) {
		super(message);
	}
	
	public DBException(Throwable cause) {
		super(cause);
	}
	
	public DBException(String message, Throwable cause) {
		super(message, cause); 
	}
	
	public DBException(String message, Throwable cause, 
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
