package com.exportstaging.activemq;

import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.connectors.messagingqueue.ActiveMQSpringConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jolokia.client.J4pClient;
import org.jolokia.client.request.J4pExecRequest;
import org.jolokia.client.request.J4pReadRequest;
import org.jolokia.client.request.J4pReadResponse;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.exportstaging.common.ExportMiscellaneousUtils.TAB_DELIMITER;

@Component("exportActiveMQUtils")
public class ExportActiveMQUtils {

    private static final String KEY_MEMORY = "Memory";
    private static final String KEY_STORE = "Store";
    private static final String KEY_TEMP = "Temp";
    private final static Logger logger = LogManager.getLogger("exportstaging");
    @Autowired
    private ActiveMQSpringConnection activeMQSpringConnection;
    @Value("${activemq.url.host}")
    private String connectionHost;
    @Value("${activemq.username}")
    private String activeMqUserName;
    @Value("${activemq.password}")
    private String activeMqPassword;
    @Value("${activemq.jolokia.port}")
    private String jolokiaPort;
    @Value("${activemq.virtual.topic.core}")
    private String exportCoreTopicPrefix;
    @Value("${activemq.virtual.topic.project}")
    private String exportProjectTopicPrefix;
    @Value("${activemq.broker.name}")
    private String brokerName;
    @Value("${activemq.queue.prefix.core}")
    private String queuePrefixCore;
    @Value("${activemq.queue.prefix.project}")
    private String queuePrefixProject;
    @Value("${export.database.name}")
    private String exportDatabaseName;

    private J4pClient client = null;
    private Date exportStartTime;

    public ExportActiveMQUtils() {
        exportStartTime = new Date();
    }

    /**
     * Printing on console and logging status of subscriber(s) based on passed exportDbName
     *
     * @param exportDbName Project specific configurable export database name
     */
    public void printQueueSubscriberInfo(String exportDbName) {
        try {
            String info;
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd@HH.mm");
            Collection<String> queueNames = getQueueObjectNameMap(exportDbName).values();
            String exportUpTime = "ExportStaging Uptime: " + getExportUpTime((new Date().getTime() - exportStartTime.getTime()) / 1000);
            System.out.println(exportUpTime);
            logger.info(exportUpTime);
            for (String objectName : queueNames) {
                String queueName = getQueueName(objectName);
                String subscriberName = getPrintableSubscriberName(queueName);
                String pendingMessageInfo = "    Pending Messages: " + getPendingMessage(objectName);
                JSONArray subscriptionsJsonArray = getValueUsingJolokiaRestApi(objectName, "Subscriptions");
                if (subscriptionsJsonArray != null && subscriptionsJsonArray.size() > 0) {
                    info = dateFormat.format(new Date()) + "  " + subscriberName + "  [Active]";
                } else {
                    info = dateFormat.format(new Date()) + "  " + subscriberName + "  [Offline]";
                }
                System.out.println(info + pendingMessageInfo);
                logger.info(info + pendingMessageInfo);
            }
        } catch (Exception e) {
            String message = "[ActiveMQ Broker] Exception while getting Subscription List.";
            logError(message, e, exportDbName);
        }
    }

    /**
     * Delete the queue from activeMQ if exist
     *
     * @param queueName Name of deleting queue
     */
    public void deleteQueue(String queueName) {
        String subscriberName = getPrintableSubscriberName(queueName);
        try {
            executeOperationUsingJolokiaRestApi("org.apache.activemq:brokerName=" + brokerName + ",type=Broker", "removeQueue", queueName);
            String logMessage = "[" + subscriberName + "] : Non-durable queue is removed from message bus system";
            System.out.println(logMessage);
            logger.info(logMessage);
        } catch (Exception e) {
            String message = "[" + subscriberName + "] : Export Error while deleting non-durable queue from message bus system.";
            logError(message, e, queueName);
        }
    }

    /**
     * Delete all the previous existing(invalid) messages from queues based on pass parameter
     *
     * @param exportDbName Project specific configurable export database name
     * @throws ExportStagingException
     */
    public void purgeAllQueues(String exportDbName) throws ExportStagingException {
        Map<String, String> queueObjectNameMap = getQueueObjectNameMap(exportDbName);
        String queueName = "";
        try {
            for (Map.Entry<String, String> entry : queueObjectNameMap.entrySet()) {
                queueName = entry.getKey();
                executeOperationUsingJolokiaRestApi(entry.getValue(), "purge");
                System.out.println(queueName + " Queue data is purge");
                logger.info(queueName + " Queue data is purge");
            }
        } catch (Exception e) {
            logError("Error while purging the queue", e, queueName);
            throw new ExportStagingException(e);
        }
    }

    /**
     * Returns the status of ActiveMQ (message bug) is running or not
     *
     * @return true if ActiveMQ is running otherwise false
     */
    public boolean isMessageBusRunning() {
        return activeMQSpringConnection.isMessageBusRunning();
    }

    private void initializeActiveMQJolokiaClient() throws ExportStagingException {
        try {
            do {
                if (client == null) {
                    String host = connectionHost;
                    if (host.contains(":"))
                        host = host.substring(0, host.indexOf(":"));
                    client = J4pClient.url("http://" + host + ":" + jolokiaPort + "/api/jolokia").user(activeMqUserName).password(activeMqPassword).build();
                }
            } while (client == null);
        } catch (Exception e) {
            logError("Exception while initializing ActiveMQJolokiaClient object", e, "ActiveMQ Utils");
            throw new ExportStagingException("[ActiveMQ Utils] Exception while initializing ActiveMQJolokiaClient object. Error Cause: " + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
        }
    }

    private <V> V getValueUsingJolokiaRestApi(String objectName, String attribute) {
        J4pReadResponse response;
        V result = null;
        try {
            initializeActiveMQJolokiaClient();
            J4pReadRequest request = new J4pReadRequest(objectName, attribute);
            response = client.execute(request);
            result = response.getValue();
        } catch (Exception e) {
            String message = "[ActiveMQ Utils] Exception while getting " + attribute;
            logError(message, e, objectName);
        }
        return result;
    }

    private void executeOperationUsingJolokiaRestApi(String objectName, String operation, String... queueName) throws ExportStagingException {
        try {
            initializeActiveMQJolokiaClient();
            J4pExecRequest request = new J4pExecRequest(objectName, operation, queueName);
            client.execute(request);
        } catch (Exception e) {
            logError("[ActiveMQ Utils] Exception while getting " + operation, e, objectName);
            throw new ExportStagingException(e);
        }
    }

    private Long getPendingMessage(String subscriberObjectName) {
        return getValueUsingJolokiaRestApi(subscriberObjectName, "QueueSize");
    }

    private Map<String, String> getQueueObjectNameMap(String exportDbName) {
        Map<String, String> queueObjectNameMap = new HashMap<>();
        JSONArray jsonArray = getValueUsingJolokiaRestApi("org.apache.activemq:brokerName=" + brokerName + ",type=Broker", "Queues");
        for (Object object : jsonArray) {
            JSONObject jsonObject = (JSONObject) object;
            String objectName = jsonObject.get("objectName").toString();
            String queueName = getQueueName(objectName);
            if (isProjectSpecificQueue(queueName, exportDbName)) {
                queueObjectNameMap.put(queueName, objectName);
            }
        }
        return queueObjectNameMap;
    }

    private boolean isProjectSpecificQueue(String queueName, String exportDbName) {
        String exportDbNameFromQueue = queueName.substring(queueName.lastIndexOf(".") + 1);
        return exportDbName.equalsIgnoreCase(exportDbNameFromQueue);
    }

    private String getPrintableSubscriberName(String queueName) {
        if (queueName.contains(queuePrefixCore)) {
            queueName = queueName.replaceAll(Pattern.quote(queuePrefixCore) + "(.+)_" + Pattern.quote(exportDatabaseName) + ".+", "$1");
        } else {
            queueName = queueName.replaceAll(Pattern.quote(queuePrefixProject) + "(.+)_" + Pattern.quote(exportDatabaseName) + ".+", "$1");
        }
        return queueName;
    }

    private String getQueueName(String objectName) {
        return getValueUsingJolokiaRestApi(objectName, "Name");
    }

    private String getExportUpTime(long seconds) {
        String exportUpTime = "";
        int day = (int) TimeUnit.SECONDS.toDays(seconds);
        long hours = TimeUnit.SECONDS.toHours(seconds) -
                TimeUnit.DAYS.toHours(day);
        long minute = TimeUnit.SECONDS.toMinutes(seconds) -
                TimeUnit.DAYS.toMinutes(day) -
                TimeUnit.HOURS.toMinutes(hours);
        long second = TimeUnit.SECONDS.toSeconds(seconds) -
                TimeUnit.DAYS.toSeconds(day) -
                TimeUnit.HOURS.toSeconds(hours) -
                TimeUnit.MINUTES.toSeconds(minute);

        if (day != 0) {
            exportUpTime += day + " day(s) ";
        }
        if (hours != 0) {
            exportUpTime += hours + " hour(s) ";
        }
        if (minute != 0) {
            exportUpTime += minute + " minute(s) ";
        }
        if (second != 0) {
            exportUpTime += second + " second(s) ";
        }
        return exportUpTime;
    }

    private void logError(String message, Exception e, String component) {
        logger.error("[" + component + "]:" + message + " Error Message:" + e.getMessage());
        logger.debug("[" + component + "]:" + message + " Error Message:" + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
    }
}
