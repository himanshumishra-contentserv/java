package com.exportstaging.producers;

import com.exportstaging.connectors.messagingqueue.ActiveMQSpringConnection;
import com.exportstaging.moderators.ModeratorData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

public class MasterProducer extends ModeratorData implements Producer {
    @Autowired
    ActiveMQSpringConnection activeMQSpringConnection;
    @Autowired
    private TaskScheduler masterProducerScheduler;

    protected String producerName;
    MessageProducer messageProducer;
    private Map<String, Session> producerSession = new HashMap<>();

    @Override
    public boolean sendMessage(MapMessage message, int JMSPriority) {
        try {
            if (activeMQSpringConnection.isMessageBusRunning()) {
                messageProducer.send(message, DeliveryMode.PERSISTENT, JMSPriority, 0);
            } else {
                while (!activeMQSpringConnection.isMessageBusRunning()) {
                    Thread.sleep(1000);
                }
                return sendMessage(message, JMSPriority);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (JMSException e) {
            handleJMSException(e);
            return false;
        }
        return true;
    }

    @Override
    public void startProducer() {
        producerName = this.getClass().getSimpleName();
        String msgInfo = "[" + producerName + "] : initializing...";
        System.out.println(msgInfo);
        logger.info(msgInfo);
        String topicName = activeMQSpringConnection.getTopicName(exportDatabaseName, false);
        messageProducer = activeMQSpringConnection.getProducer(topicName, producerName, getProducerSession());
    }

    @Override
    public void stopProducer() {
        closeSession();
    }

    @Override
    public MapMessage getMapMessage() {
        Session session = producerSession.get(producerName);
        try {
            if (session != null) {
                return session.createMapMessage();
            }
        } catch (JMSException e) {
            logError("JMSException while creating MapMessage object.", e, producerName);
        }
        return null;
    }

    @Override
    public TaskScheduler getTaskScheduler() {
        return masterProducerScheduler;
    }

    @Override
    public String getProducerName() {
        return producerName;
    }

    @Override
    public int getProducerID() {
        return masterProducerID;
    }

    Session getProducerSession() {
        if (producerSession.get(producerName) == null) {
            producerSession.put(producerName, activeMQSpringConnection.createProducerSession(this.getClass().getName() + "_" + exportDatabaseName));
            logger.info("[" + producerName + "] : Session Created");
        }
        return producerSession.get(producerName);
    }

    private void closeSession() {
        if (producerSession != null) {
            try {
                producerSession.get(producerName).close();
                producerSession.remove(producerName);
                logger.info("[" + producerName + "] : session closed");
            } catch (JMSException e) {
                logError("Exception while closing producer session.", e, producerName);
            }
        }
    }

    private void handleJMSException(JMSException e) {
        String exceptionMessage = e.getMessage();
        if (exceptionMessage.contains(exceptionMessageStorageFull)) {
            try {
                Thread.sleep(30000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            logError("Exception while sending message to JMS(activemq). Please check all the consumers are in running state or not", e, producerName);
        } else {
            logError("Exception while sending message to JMS(activemq).", e, producerName);
        }
    }
}