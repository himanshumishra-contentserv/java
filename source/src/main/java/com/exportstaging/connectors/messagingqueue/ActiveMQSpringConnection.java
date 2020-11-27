package com.exportstaging.connectors.messagingqueue;

import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.pool.PooledConnection;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.jms.*;
import javax.jms.Queue;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.exportstaging.common.ExportMiscellaneousUtils.*;

@Component("activeMQSpringConnection")
public class ActiveMQSpringConnection {
    private volatile boolean isMessageBusRunning = false;
    private Connection connection = null;
    private PooledConnectionFactory pooledConnectionFactory = null;
    private Topic topic = null;
    private Map<String, MessageConsumer> messageConsumers = new HashMap<>();
    private Map<String, MessageProducer> messageProducers = new HashMap<>();

    @Autowired
    private ActiveMQMBeanConnection mqmBeanConnection;
    @Value("${activemq.connection.client}")
    private String clientName;
    @Value("${core.project.name}")
    private String projectName;
    @Value("${activemq.url.host}")
    private String connectionHost;
    @Value("${activemq.username}")
    private String userName;
    @Value("${activemq.password}")
    private String password;
    @Value("${activemq.redelivery.delay}")
    private int redeliveryDelay;
    @Value("${activemq.redelivery.initial.delay}")
    private int redeliveryInitialDelay;
    @Value("${activemq.subscriber.master}")
    protected String masterSubscriber;
    @Value("${activemq.subscriber.elastic}")
    protected String elasticSubscriber;
    @Value("${activemq.virtual.topic.core}")
    private String exportCoreVirtualTopic;
    @Value("${activemq.virtual.topic.project}")
    private String exportProjectVirtualTopic;
    @Value("${activemq.message.selector.jmstype}")
    private String jmsTypeSelector;
    @Value("${activemq.durable.queue.prefetchSize}")
    private String durableQueuePrefetchSize;
    @Value("${activemq.queue.prefix.core}")
    private String queuePrefixCore;
    @Value("${activemq.queue.prefix.project}")
    private String queuePrefixProject;
    @Value("${export.database.name}")
    private String exportDatabaseName;

    private final static Logger logger = LogManager.getLogger("exportstaging");

    public ActiveMQSpringConnection() {
    }

    public void createCustomConnection(String clientName) {
        setClientName(clientName);
        createConnection();
    }

    public Session createSession(String clientName, boolean isDurable) {
        try {
            int ackMode = Session.CLIENT_ACKNOWLEDGE;
            if (!isDurable) {
                ackMode = Session.AUTO_ACKNOWLEDGE;
            }
            return createPoolConnection(clientName).createSession(false, ackMode);
        } catch (JMSException e) {
            logError("JMSException while creating consumer session object", e);
        } catch (Exception e) {
            logError("Exception while creating consumer session object.", e);
        }
        return null;
    }

    public Session createProducerSession(String connectionClientName) {
        try {
            return createPoolConnection(connectionClientName).createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            logError("JMSException while creating Producer session object.",e);
        } catch (Exception e) {
            logError("Exception while creating Producer session object.",e);
        }
        return null;
    }

    public Connection getConnection() {
        if (connection == null) {
            createConnection();
        }
        return connection;
    }

    /**
     * This method return the MessageConsumer object
     *
     * @param exportDbName    Working project name
     * @param subscriberName Name of the subscriber for which to create consumer
     * @param session        ActiveMQ Session object
     * @param itemTypes      List of subscriber supported item types to create selector
     * @return Return the MessageConsumer object
     */
    public MessageConsumer getConsumer(String exportDbName, String subscriberName, Session session, List itemTypes, boolean isDurable, boolean isCustomSubscriber) {
        if (messageConsumers.get(subscriberName + "_" + exportDbName) == null) {
            createMessageConsumer(exportDbName, subscriberName, session, itemTypes, isDurable, isCustomSubscriber);
        }
        return messageConsumers.get(subscriberName + "_" + exportDbName);
    }

    /**
     * This method return the MessageProducer object based on topic name and producerName
     *
     * @param topicName    Name of the export database topic
     * @param producerName Name of the producer like. MasterProducer
     * @param session      ActiveMQ session object
     * @return MessageProducer object
     */
    public MessageProducer getProducer(String topicName, String producerName, Session session) {
        if (messageProducers.get(producerName) == null) {
            createMessageProducer(topicName, producerName, session);
        }
        return messageProducers.get(producerName);
    }

    /**
     * Close the messaging bus connection and clear the pooled connection factory.
     */
    public void close() {
        try {
            if (connection != null) {
                connection.close();
                pooledConnectionFactory.clear();
                logger.info(clientName + " Stopped");
                System.out.println(clientName + " Stopped");
            }
        } catch (JMSException e) {
            logError("JMSException while closing bus system connection.", e);
        } catch (Exception e) {
            logError("Exception while closing bus system connection.",e);
        }
    }

    /**
     * Closed or stop the running message consumer based on clientID and remove it from the messageConsumers map.
     *
     * @param clientID Consumer name which is register in the message system
     */
    public void closeConsumer(String clientID) {
        try {
            clientID = clientID + "_" + projectName;
            if (messageConsumers != null) {
                if (messageConsumers.get(clientID) != null) {
                    messageConsumers.get(clientID).close();
                    messageConsumers.remove(clientID);
                    logger.info("[" + clientID + "]: Subscriber is closed");
                }
            }
        } catch (JMSException e) {
            logError("JMSException while closing consumer connection for " + clientID, e);
        }
    }

    public boolean isMessageBusRunning() {
        return isMessageBusRunning;
    }

    /**
     * Return the export database virtual topic name based on project name.
     *
     * @param exportDbName Supported CS project name.
     * @return Name of the virtual topic
     */
    public String getTopicName(String exportDbName, boolean isProjectProducer) {
        if (isProjectProducer) {
            return exportProjectVirtualTopic + "." + exportDbName;
        } else {
            return exportCoreVirtualTopic + "." + exportDbName;
        }
    }

    public String getExportDatabaseName() {
        return exportDatabaseName;
    }

    /**
     * Return the export database messaging system queue name based on project name and subscriber name.
     *
     * @param exportDbName   Supported CS project name.
     * @param subscriberName Name of the subscriber it may be core/custom subscriber like. ElasticSubscriber/MasterSubscriber/ProjectSubscriber
     * @return Name of the queue
     */
    public String getQueueName(String exportDbName, String subscriberName, boolean isProjectSubscriber) {
        String topicName = getTopicName(exportDbName, isProjectSubscriber);
        String queueName;
        if (isProjectSubscriber) {
            queueName = queuePrefixProject + subscriberName + "_" + exportDbName + "." + topicName;
        } else {
            queueName = queuePrefixCore + subscriberName + "_" + exportDbName + "." + topicName;
        }
        return queueName;
    }

    private void setClientName(String clientName) {
        this.clientName = clientName;
    }

    private void createConnection() {
        String loggerMessage = "------------------ActiveMQ Connection Initialization--------------------";
        System.out.println(loggerMessage);
        logger.info(loggerMessage);
        if (!clientName.contains("_" + exportDatabaseName)) {
            clientName += "_" + exportDatabaseName;
        }
        String url = getBrokerUrl();
        System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES", "*");
        try {
            String log = "Trying to Connect to ActiveMQ, host name: " + connectionHost + " , Connection Client name: " + clientName;
            System.out.println(log);
            logger.info(log);
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
            RedeliveryPolicy redeliveryPolicy = factory.getRedeliveryPolicy();
            redeliveryPolicy.setMaximumRedeliveries(RedeliveryPolicy.NO_MAXIMUM_REDELIVERIES);
            redeliveryPolicy.setInitialRedeliveryDelay(redeliveryInitialDelay);
            redeliveryPolicy.setRedeliveryDelay(redeliveryDelay);
            pooledConnectionFactory = new org.apache.activemq.pool.PooledConnectionFactory(factory);
            pooledConnectionFactory.setMaxConnections(10);
            connection = ((PooledConnection) pooledConnectionFactory
                    .createConnection(userName, password))
                    .getConnection();
            connection.setClientID(clientName);
            ((ActiveMQConnection) connection).addTransportListener(new TransportListener() {
                @Override
                public void onCommand(Object command) {
                }

                @Override
                public void onException(IOException error) {
                }

                @Override
                public void transportInterupted() {
                    setMessageBusRunning(false);
                }

                @Override
                public void transportResumed() {
                    setMessageBusRunning(true);
                    try {
                        mqmBeanConnection.createBrokerViewMBean();
                    } catch (ExportStagingException e) {
                        logError("ExportStagingException  while createBrokerViewMBean.", e);
                    }
                }
            });
            connection.start();
            setMessageBusRunning(true);
            System.out.println(clientName + " Connected to ActiveMQ\n");
            logger.info(clientName + " Connected to ActiveMQ");
        } catch (InvalidClientIDException e) {
            logError("Export database already running for " + projectName + " project.Another session cannot be started for the same!", e);
            System.out.println("Export database already running for " + projectName + " project.Another session cannot be started for the same!");
            System.exit(0);
        } catch (JMSSecurityException e) {
           logError("ActiveMQ username or password is invalid try with correct credentials", e);
            System.exit(0);
        } catch (JMSException e) {
            logError("JMSException on Create Bus System connection.", e);
        } catch (Exception e) {
            logError("Exception on Create Bus System connection.", e);
        }
    }

    private Connection createPoolConnection(String connectionClientName) throws JMSException {
        if (pooledConnectionFactory == null) {
            if (!connectionClientName.equals(elasticSubscriber) && !connectionClientName.equals(masterSubscriber)) {
                createCustomConnection(connectionClientName);
            } else {
                createConnection();
            }
        }
        if (connection == null) {
            Connection connection = pooledConnectionFactory.createConnection();
            connection.setClientID(connectionClientName);
            connection.start();
        }
        return connection;
    }

    private Topic createTopic(String topicName) {
        return new ActiveMQTopic(topicName);
    }

    private Topic getTopic(String topic) {
        return createTopic(topic);
    }

    private void createMessageConsumer(String exportDbName, String subscriberName, Session session, List itemTypes, boolean isDurable, boolean isCustomSubscriber) {
        try {
            MessageConsumer messageConsumer;
            String queueName = getQueueName(exportDbName, subscriberName, isCustomSubscriber);
            if (isDurable) {
                queueName = queueName + "?consumer.prefetchSize=100";
            }
            Queue queue = session.createQueue(queueName);
            messageConsumer = session.createConsumer(queue);

            messageConsumers.put(subscriberName + "_" + exportDbName, messageConsumer);
            System.out.println("[" + subscriberName + "] :" + " Started");
            logger.info(subscriberName + " Started");
        } catch (JMSException e) {
            logError("JMSException while creating " + subscriberName + " consumer.", e);
        } catch (Exception e) {
            logError("Exception while creating " + subscriberName + " consumer.", e);
        }
    }

    private String getSelector(List itemTypes) {
        String selector = "";
        if (itemTypes != null) {
            Iterator iterator = itemTypes.iterator();
            while (iterator.hasNext()) {
                selector += jmsTypeSelector + " = '" + iterator.next() + "'";
                if (iterator.hasNext()) {
                    selector += " OR ";
                }
            }
            selector += " OR " + jmsTypeSelector + " = '" + ExportMiscellaneousUtils.EXPORT_TYPE_OPERATION + "'";
        }
        return selector;
    }

    private void createMessageProducer(String topic, String producerName, Session session) {
        try {
            MessageProducer messageProducer = session.createProducer(getTopic(topic));
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            messageProducers.put(producerName, messageProducer);
            System.out.println("[" + producerName + "] :" + " Started");
            logger.info("[" + producerName + "]" + " Started");
        } catch (JMSException e) {
            logError("JMSException while creating " + producerName + " message producer.", e);
        } catch (Exception e) {
            logError("Exception while creating " + producerName + " message producer.", e);
        }
    }

    private void setMessageBusRunning(boolean messageBusRunning) {
        String log = "[ActiveMQ] ActiveMQ ";
        if (messageBusRunning) {
            log += "Started";
        } else {
            log += "Stopped";
        }
        logger.info(log);
        isMessageBusRunning = messageBusRunning;
    }

    /**
     * Prepares ActiveMQ broker url
     * If connection port number is not mentioned in the URL then default port 61616 is used
     *
     * @return String broker url with provided hostname and port
     */
    private String getBrokerUrl() {
        String ipPortPair[] = connectionHost.split(":");
        String ipAddress = ipPortPair[0].trim();
        int port = ((ipPortPair.length == 1) ? ExportMiscellaneousUtils.DEFAULT_PORT_ACTIVEMQ : Integer.parseInt(ipPortPair[1].trim()));
        connectionHost = ipAddress + ":" + port;
        return "failover:(tcp://" + connectionHost + ")";
    }

    private void logError(String message, Exception e) {
        logger.error("[ActiveMQ]:" + message + " Error Message:" + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
    }
}
