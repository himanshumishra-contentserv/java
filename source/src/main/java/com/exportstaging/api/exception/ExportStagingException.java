package com.exportstaging.api.exception;

/**
 * ExportStagingException for Export Staging APIs.
 */
public class ExportStagingException extends Exception {


    public static String CONNECTION_EXCEPTION = "ConnectionException";
    //TODO Add Exception Messages and Exception Codes
    private String exceptionMessage = null;
    private int errorId = 0;

    public ExportStagingException() {
        super();
    }

    public ExportStagingException(String message) {
        super(message);
        this.exceptionMessage = message;
    }

    public ExportStagingException(String exceptionMessage, String errorType) {
        super(exceptionMessage);
        this.exceptionMessage = exceptionMessage;
        if (CONNECTION_EXCEPTION.equals(errorType)) {
            errorId = 1;
        }
    }

    public ExportStagingException(Throwable cause) {
        super(cause);
        this.exceptionMessage = cause.getMessage();
    }

    public ExportStagingException(Exception e) {
        super(e);
        this.exceptionMessage = e.getMessage();
    }

    public ExportStagingException(String message, Throwable cause) {
        super(message, cause);
        this.exceptionMessage = message;
    }


    @Override
    public String getMessage() {
        //Following if condition ensures that if exceptionMessage var is not initialized, then return
        //exception from super
        if (exceptionMessage == null) {
            exceptionMessage = super.getMessage();
        }
        return exceptionMessage;
    }

    /**
     * Indicates if the exception is due to connection error
     *
     * @return True, if connection Exception, else false
     */
    public boolean isConnectionException() {
        return errorId == 1;
    }
}
