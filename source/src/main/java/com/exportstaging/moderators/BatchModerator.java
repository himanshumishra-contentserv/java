package com.exportstaging.moderators;

import com.exportstaging.activemq.ExportActiveMQUtils;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.connectors.messagingqueue.ActiveMQMBeanConnection;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.producers.Producer;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import java.util.*;

@Scope("prototype")
@Component("batchModerator")
public class BatchModerator extends ModeratorData implements Runnable {
    private final static Logger logger = LogManager.getLogger("exportstaging");
    private String itemType = null;
    private String producerName = null;
    private Producer producer;
    private int deleteStatus;
    @Autowired
    private ExportActiveMQUtils exportActiveMQUtils;
    @Autowired
    private ActiveMQMBeanConnection mBeanConnection;

    @Value("${export.producer.sleep.time}")
    private int producerSleepTime;
    @Value("${activemq.broker.store.threshold}")
    private int maxStoreUsage;
    @Value("${core.project.name}")
    private String projectName;

    private static boolean isInitialOperation = false;

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public String getItemType() {
        return itemType;
    }

    public void setProducer(Producer producer) {
        this.producer = producer;
        this.producerName = producer.getProducerName();
    }

    public int getDeleteStatus() {
        return deleteStatus;
    }

    public void setDeleteStatus(int deleteStatus) {
        this.deleteStatus = deleteStatus;
    }

    @Override
    public void run() {
        ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
        Thread.currentThread().setName(producerName + "_" + itemType);
        if (isInitialOperation) {
            return;
        }
        if (exportActiveMQUtils.isMessageBusRunning()) {
            boolean shouldProcess = true;
            int processedCount;
            do {
                try {
                    if (!itemType.equals(ExportMiscellaneousUtils.EXPORT_TYPE_OPERATION)) {
                        getMessagesInBatch(mTypeConfiguration);
                        getMessagesInBatch(mTypeMapping);
                    }
                    processedCount = getMessagesInBatch(mTypeItem);
                    if (itemType.equals(ITEMTYPE_FILE)) {
                        getMessagesInBatch(mTypeFileContent);
                    }
                    if (processedCount < mResultBatchSize) {
                        shouldProcess = false;
                    }
                } catch (ExportStagingException e) {
                    logError("Exception while getting messages in batch.", e, producerName);
                }
            } while (shouldProcess);
        }
        ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
    }

    private int getMessagesInBatch(String type) throws ExportStagingException {
        Map<String, Object> producerSettings = getProducerSettings(type, itemType);
        String sSelectQuery;
        int iResultSize = 0;
        int iUpdateCount = 0;
        List<Map<String, Object>> resultRows;
        do {
            if (mBeanConnection.getStorePercentUsage(producerName) < maxStoreUsage) {
                sSelectQuery = getSelectQuery(type, producerSettings);
                resultRows = intermediateDAO.fetchData(sSelectQuery);
                iResultSize = resultRows != null ? resultRows.size() : 0;
                if (iResultSize != 0) {
                    setMessages(resultRows, type, itemType);
                    iUpdateCount += iResultSize;
                }
            } else {
                logger.debug("ActiveMQ kahadb storage capacity get full to " + maxStoreUsage + "%");
            }
        } while (iResultSize == mResultBatchSize);
        return iUpdateCount;
    }

    private String getSelectQuery(String type, Map<String, Object> producerSettings) {
        String sSelectQuery = "SELECT *" +
                " FROM " + producerSettings.get(CONSTANT_TABLENAME) +
                " WHERE (" + columnAction + " = " + mActionUpdate +
                " OR " + columnAction + " = " + mActionDelete +
                " OR " + columnAction + " = " + mActionCreate +
                " OR " + columnAction + " = " + mActionInitial +
                ") AND (" + columnProducerStatus + " & " + producer.getProducerID() + " = 0 " +
                ") ORDER BY " + columnJmsPriority + " DESC ";
        if (mTypeMapping.equals(type)) {
            sSelectQuery += ", " + columnID + " ASC ";
        }
        sSelectQuery += "LIMIT " + mResultBatchSize + ";";
        return sSelectQuery;
    }

    private void setMessages(List<Map<String, Object>> resultRows, String type, String itemType) throws ExportStagingException {
        List<String> listUpdated = new ArrayList<>();
        String itemID;
        String itemMessage;
        int action, jobID;
        long maxTimeValue = 0;
        long timeValueFromDB;
        Map<String, Object> SettingMap = getProducerSettings(type, itemType);
        String tableName = (String) SettingMap.get(CONSTANT_TABLENAME);
        try {
            ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
            MapMessage message = producer.getMapMessage();
            if (message != null) {
                for (Map rowMap : resultRows) {
                    long startTime = System.currentTimeMillis();
                    timeValueFromDB = Long.parseLong(rowMap.get(columnInsertTime).toString());
                    if (maxTimeValue < timeValueFromDB) {
                        maxTimeValue = timeValueFromDB;
                    }
                    itemID = rowMap.get(columnID).toString();
                    action = Integer.parseInt(rowMap.get(columnAction).toString());
                    jobID = Integer.parseInt(rowMap.get(columnJobID).toString());
                    itemMessage = rowMap.get(columnMessage).toString();

                    if (itemType.equals(ExportMiscellaneousUtils.EXPORT_TYPE_OPERATION)) {
                        handleOperationMessage(itemMessage, action);
                    }
                    message.setString(columnID, itemID);
                    message.setString(columnMessage, itemMessage);
                    message.setString(ExportMiscellaneousUtils.CONSTANT_ITEM_TYPE, itemType);
                    message.setString(ExportMiscellaneousUtils.CONSTANT_TYPE, type);
                    message.setLong(ExportMiscellaneousUtils.IDB_INSERT_TIME, maxTimeValue);
                    message.setInt(columnAction, action);
                    message.setInt(columnJobID, jobID);
                    message.setInt(columnVersionNr, Integer.parseInt(rowMap.get(columnVersionNr).toString()));
                    message.setJMSType(itemType);
                    message.setStringProperty("ObjectId", itemID);

                    int jmsPriority = Integer.parseInt(rowMap.get(columnJmsPriority).toString());
                    if (jmsPriority == ExportMiscellaneousUtils.CONSTANT_EXPORT_TYPE_INITIAL) {
                        message.setInt(ExportMiscellaneousUtils.CONSTANT_EXPORT_TYPE, ExportMiscellaneousUtils.CONSTANT_EXPORT_TYPE_INITIAL);
                        jmsPriority = (int) SettingMap.get(CONSTANT_PRIORITY);
                    } else {
                        message.setInt(ExportMiscellaneousUtils.CONSTANT_EXPORT_TYPE, ExportMiscellaneousUtils.CONSTANT_EXPORT_TYPE_DELTA);
                    }
                    message.setJMSCorrelationID((String) SettingMap.get(CONSTANT_CORRELATIONID));

                    ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_OPERATION_TYPE, ExportMiscellaneousUtils.getOperationType(action, jmsPriority, type));
                    ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_TRACE_ID, String.valueOf(maxTimeValue));
                    String loggerInfo = "with ID: " + itemID + " and JMSPriority: " + jmsPriority + ". Took(ms): ";
                    long tookTime;
                    if (producer != null && producer.sendMessage(message, jmsPriority)) {
                        listUpdated.add(itemID);
                        tookTime = System.currentTimeMillis() - startTime;
                        logger.info("[" + producerName + "] Message sent " + loggerInfo + tookTime);
                    } else {
                        logger.error("[" + producerName + "] Message sending failed " + loggerInfo);
                    }
                }
            }
        } catch (JMSException | ExportStagingException e) {
            logError("Exception while sending message.", e, producerName);
        } finally {
            ThreadContext.clearAll();
        }
        if (!listUpdated.isEmpty()) {
            updateStatus(tableName, StringUtils.join(listUpdated, ","), maxTimeValue);
            deleteFromIntermediateTable(tableName);
        }
    }

    private void handleOperationMessage(String itemMessage, int action) throws ExportStagingException {
        if (action == mActionInitial) {
            isInitialOperation = true;
            try {
                exportActiveMQUtils.purgeAllQueues(exportDatabaseName);
            } catch (Exception e) {
                logError("Exception while purging all the messages from message bus", e, producerName);
                System.out.println("[" + producerName + "] : Exception while purging all the messages from message bus");
                exportActiveMQUtils.purgeAllQueues(exportDatabaseName);
            }
            isInitialOperation = false;
        } else if (action == mActionUpdate) {
            String logLevel = (String) ((JSONObject) JSONValue.parse(itemMessage)).get(ExportMiscellaneousUtils.EXPORT_JSON_KEY_DEBUG_MODE);
            if ("DEBUG".equals(logLevel)) {
                updateDebugLoggerMode(Level.DEBUG);
            } else if ("OFF".equals(logLevel)) {
                updateDebugLoggerMode(Level.OFF);
            }
        }
    }

    /**
     * Method updates the debug mode level based on CS UI debug mode enable disable option.
     *
     * @param debugLevel Debug mode logger level.
     */
    private void updateDebugLoggerMode(Level debugLevel) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        LoggerConfig rootLoggerConfig = config.getLoggers().get(ExportMiscellaneousUtils.EXPORT_LOGGER_NAME);

        rootLoggerConfig.removeAppender("debug-log");
        rootLoggerConfig.addAppender(config.getAppender("debug-log"), debugLevel, null);
        ctx.updateLoggers();
    }

    private void deleteFromIntermediateTable(String tableName) throws ExportStagingException {
        String sDeleteQuery = "DELETE FROM " + tableName +
                " WHERE " + columnProducerStatus + " = " + getDeleteStatus() + ";";
        intermediateDAO.executeQuery(sDeleteQuery);
    }

    private void updateStatus(String tableName, String iDList, long maxTimeValue) throws ExportStagingException {
        String sUpdateQuery = "UPDATE " + tableName +
                " SET " + columnProducerStatus + "=" + columnProducerStatus + "+" + producer.getProducerID() +
                " WHERE " + columnID +
                " IN (" + iDList + ")" +
                " AND " + columnInsertTime + " <= " + maxTimeValue + ";";
        intermediateDAO.executeQuery(sUpdateQuery);
    }
}