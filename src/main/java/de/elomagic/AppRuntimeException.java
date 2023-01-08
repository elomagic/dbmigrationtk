package de.elomagic;

public class AppRuntimeException extends RuntimeException {

    public AppRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public AppRuntimeException(String message) {
        super(message);
    }

}
