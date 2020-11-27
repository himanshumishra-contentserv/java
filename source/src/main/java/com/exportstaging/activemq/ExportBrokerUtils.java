package com.exportstaging.activemq;

import com.exportstaging.common.UnitConverter;
import com.exportstaging.connectors.messagingqueue.ActiveMQSpringConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.SimpleDateFormat;
import java.util.*;
import static com.exportstaging.common.ExportMiscellaneousUtils.*;
public class ExportBrokerUtils {
    private static final String KEY_MEMORY = "Memory";
    private static final String KEY_STORE = "Store";
    private static final String KEY_TEMP = "Temp";
    private final static Logger logger = LogManager.getLogger("exportstaging");
    @Autowired
    BrokerService brokerService;
    @Autowired
    UnitConverter unitConverter;
    @Autowired
    private ActiveMQSpringConnection activeMQSpringConnection;
    private List<Subscription> subscriptionList = new ArrayList<>();

    public ExportBrokerUtils() {
    }

    public void startBroker() {
        String loggerMessage = "------------------ActiveMQ Broker Initialization--------------------";
        System.out.println("\n" + loggerMessage);
        logger.info(loggerMessage);
        try {
            brokerService.start();
            loggerMessage = "ActiveMQ Broker " + brokerService.getBrokerName() + " Started";
            System.out.println(loggerMessage);
            logger.info(loggerMessage);
            activeMQSpringConnection.getConnection();
            displayCurrentMemoryPercentUsage();
            setSubscriptionList();
        } catch (Exception e) {
            logger.error("[ActiveMQ Broker] Exception while attempting to start broker. Error cause: " + e.getMessage() + TAB_DELIMITER  + Arrays.toString(e.getStackTrace()));
        }
    }

    private void setSubscriptionList() {
        try {
            ActiveMQDestination activeMQDestination = new ActiveMQTopic("ExportTopic");
            Destination destination = brokerService.getDestination(activeMQDestination);
            subscriptionList = destination.getConsumers();
        } catch (Exception e) {
            logger.error("[ActiveMQ Broker] Exception while getting Subscription List. Error cause: " + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
        }
    }

    public void removeDurableSubscriber(String clientID, String subscriberID) {
        try {
            brokerService.getAdminView().destroyDurableSubscriber(clientID, subscriberID);
        } catch (Exception e) {
            logger.error("[ActiveMQ Broker] Exception while removing subscriber with ID: " + subscriberID + ". Error cause: " + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
        }
    }

    public String getUpTime() {
        return brokerService.getUptime();
    }

    public Map<String, Integer> getCurrentPercentUsage() {
        SystemUsage systemUsage = brokerService.getSystemUsage();
        Map<String, Integer> memoryPercent = new HashMap<>();
        memoryPercent.put(KEY_MEMORY, systemUsage.getMemoryUsage().getPercentUsage());
        memoryPercent.put(KEY_STORE, systemUsage.getStoreUsage().getPercentUsage());
        memoryPercent.put(KEY_TEMP, systemUsage.getTempUsage().getPercentUsage());
        return memoryPercent;
    }

    public int getMemoryPercentUsage() {
        SystemUsage systemUsage = brokerService.getSystemUsage();
        return systemUsage.getMemoryUsage().getPercentUsage();
    }

    public int getStorePercentUsage() {
        SystemUsage systemUsage = brokerService.getSystemUsage();
        return systemUsage.getStoreUsage().getPercentUsage();
    }

    public int getTempPercentUsage() {
        SystemUsage systemUsage = brokerService.getSystemUsage();
        return systemUsage.getTempUsage().getPercentUsage();
    }

    public void displayCurrentMemoryPercentUsage() {
        SystemUsage systemUsage = brokerService.getSystemUsage();
        MemoryUsage memoryUsage = systemUsage.getMemoryUsage();
        StoreUsage storeUsage = systemUsage.getStoreUsage();
        String loggerMessage = "---------------------ActiveMQ Memory Usages-------------------------";
        System.out.println(loggerMessage);
        logger.info(loggerMessage);
        loggerMessage = "Memory Limit: " + unitConverter.convertMemory(memoryUsage.getLimit(), UnitConverter.BYTE);
        System.out.println(loggerMessage);
        logger.info(loggerMessage);
        loggerMessage = "Memory Usage: " + unitConverter.convertMemory(memoryUsage.getUsage(), UnitConverter.BYTE);
        System.out.println(loggerMessage);
        logger.info(loggerMessage);
        loggerMessage = "Memory Usage Percent: " + memoryUsage.getPercentUsage() + " %";
        System.out.println(loggerMessage);
        logger.info(loggerMessage);

        loggerMessage = "Store Limit: " + unitConverter.convertMemory(storeUsage.getLimit(), UnitConverter.BYTE);
        System.out.println(loggerMessage);
        logger.info(loggerMessage);
        loggerMessage = "Store Usage: " + unitConverter.convertMemory(storeUsage.getUsage(), UnitConverter.BYTE);
        System.out.println(loggerMessage);
        logger.info(loggerMessage);
        loggerMessage = "Store Usage Percent: " + storeUsage.getPercentUsage() + " %";
        System.out.println(loggerMessage);
        logger.info(loggerMessage);
    }

    public void stopBroker() {
        try {
            activeMQSpringConnection.close();
            brokerService.stop();
            String loggerMessage = "ActiveMQ Broker " + brokerService.getBrokerName() + " Stopped";
            System.out.println(loggerMessage);
            logger.info(loggerMessage);
        } catch (Exception e) {
            logger.error("[ActiveMQ Broker] Exception while stopping broker. Error cause: " + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
        }
    }

    public void printSubscriberInfo() {
        try {
            String info;
            setSubscriptionList();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd@HH.mm");
            String exportUpTime = "ExportStaging Uptime: " + getUpTime();
            System.out.println(exportUpTime);
            logger.info(exportUpTime);
            for (Subscription subscription : subscriptionList) {
                String pendingMessageInfo = "        Pending Messages: " + (subscription.getPendingQueueSize() + + subscription.getInFlightSize());
                if (subscription.toString().contains("active=true")) {
                    info = dateFormat.format(new Date()) + "  " + subscription.getConsumerInfo().getSubscriptionName() + "  [Active]";
                    System.out.println(info + pendingMessageInfo);
                    logger.info(info + pendingMessageInfo);
                } else if (subscription.toString().contains("active=false")) {
                    info = dateFormat.format(new Date()) + "  " + subscription.getConsumerInfo().getSubscriptionName() + "  [Offline]";
                    System.out.println(info + pendingMessageInfo);
                    logger.info(info + pendingMessageInfo);
                }
            }
        } catch (Exception e) {
            logger.error("[ActiveMQ Broker] Exception while logging subscriber info. Error cause: " + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
        }
    }

    public List<Subscription> getSubscriptionList() {
        return subscriptionList;
    }

    public boolean isMessageBusRunning() {
        return activeMQSpringConnection.isMessageBusRunning();
    }
}
