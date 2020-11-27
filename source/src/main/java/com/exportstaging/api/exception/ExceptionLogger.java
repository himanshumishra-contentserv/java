package com.exportstaging.api.exception;

import static com.exportstaging.common.ExportMiscellaneousUtils.TAB_DELIMITER;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Logs exception
 *
 */
public class ExceptionLogger {

    private static final Logger logger = LogManager.getLogger("exportstaging");

    /**
     * Logs exception to debug and error logs
     * @param message message to log
     * @param exception exception
     * @param component where exception is thrown
     */
    public static void logError(String message, Throwable exception, String component) {
        String errorMessage = "[" + component + "]:" + message + " Error Message:" + exception.getMessage();
        logger.error(errorMessage);
        logger.debug(errorMessage + TAB_DELIMITER + ExceptionUtils.getStackTrace(exception));
    }

}
