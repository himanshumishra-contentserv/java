package com.exportstaging.api.exception;

/**
 * ExportMaterializeViewException for Export Staging.
 */

public class ExportMaterializeViewException extends ExportStagingException {

    private String exceptionMessage = null;

    public ExportMaterializeViewException() {
        super();
    }

    public ExportMaterializeViewException(String message) {
        super(message);
        this.exceptionMessage = message;
    }

    public ExportMaterializeViewException(Throwable cause) {
        super(cause);
        this.exceptionMessage = cause.getMessage();
    }

    public ExportMaterializeViewException(Exception e) {
        super(e);
        this.exceptionMessage = e.getMessage();
    }

    public ExportMaterializeViewException(String message, Throwable cause) {
        super(message, cause);
        this.exceptionMessage = message;
    }

    @Override
    public String getMessage() {
        return exceptionMessage;
    }
}
