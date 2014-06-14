package wong.spance.sparePool;

public class DBException extends RuntimeException {

    public DBException(String message) {
        super(message);
    }

    public DBException(String message, Object... args) {
        super(String.format(message, args));
    }

    public DBException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

}