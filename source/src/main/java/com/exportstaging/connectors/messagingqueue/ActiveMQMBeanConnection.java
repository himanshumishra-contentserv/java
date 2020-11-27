package com.exportstaging.connectors.messagingqueue;

import com.exportstaging.api.exception.ExportStagingException;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.PersistenceAdapterView;
import org.apache.activemq.broker.jmx.PersistenceAdapterViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.management.*;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.rmi.ConnectException;
import java.util.*;

import static com.exportstaging.common.ExportMiscellaneousUtils.TAB_DELIMITER;

@Component("activeMQMBeamConnection")
public class ActiveMQMBeanConnection {

    private final static String CONST_DESTINATION_NAME = "destinationName";
    @Value("${activemq.url.host}")
    private String hostName;
    @Value("${activemq.jmx.port}")
    private String jmxPort;
    @Value("${activemq.broker.name}")
    private String brokerName;
    @Value("${core.project.name}")
    private String projectName;
    @Value("${activemq.producer.master}")
    private String masterProducer;
    @Value("${activemq.message.selector.jmstype}")
    private String jmsTypeSelector;
    @Value("${export.tools.connection.retry}")
    private int retryCount;
    @Value("${export.tools.connection.delay}")
    private int retryDelay;
    @Value("${activemq.core.queue.size}")
    private int coreQueuesStorageSize;
    @Value("${activemq.project.queue.size}")
    private int projectQueuesStorageSize;
    @Value("${activemq.producer.project}")
    private String projectProducer;
    @Value("${activemq.broker.store.threshold}")
    private int maxStoreUsage;

    private static int count = 0;
    private final static Logger logger = LogManager.getLogger("exportstaging");
    private final static double bytesToGbDivisor = 1024.0 * 1024.0 * 1024.0;
    private static Map<String, PersistenceAdapterViewMBean> persistenceAdapterViewMBeanMap = new HashMap<>();

    private MBeanServerConnection connection;
    private BrokerViewMBean brokerViewMBean;
    private Map<String, QueueViewMBean> queueViewMBeanList = new HashMap<>();

    private MBeanServerConnection createConnection() throws ExportStagingException {
        if (count < retryCount) {
            try {
                String jmxURL = getJmxUrl();
                if (count != 0) {
                    Thread.sleep(retryDelay);
                }
                String msg = "Trying to Connect to ActiveMQ, host name(s): " + jmxURL;
                logger.info(msg);
                System.out.println(msg);
                connection = JMXConnectorFactory.connect(new JMXServiceURL(jmxURL)).getMBeanServerConnection();
                logger.info("Connected to ActiveMQ JMX Portal");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                if (e.getMessage().contains("Failed to retrieve RMIServer stub")) {
                    count++;
                    createConnection();
                }
            }
        } else {
            String msg = "ExportStaging Initialization Error: Connection to ActiveMQ was not successful. Please" +
                    " verify if ActiveMQ is properly configured and running";
            System.err.println(msg);
            logger.info(msg);
            throw new ExportStagingException(msg);
        }
        return connection;
    }

    @PostConstruct
    public BrokerViewMBean createBrokerViewMBean() throws ExportStagingException {
        try {
            if (brokerViewMBean == null) {
                String brokerObjectName = "org.apache.activemq:type=Broker,brokerName=" + brokerName;
                brokerViewMBean = MBeanServerInvocationHandler.newProxyInstance(createConnection(), new ObjectName(brokerObjectName),
                        BrokerViewMBean.class, true);
            }
        } catch (MalformedObjectNameException e) {
            String errorMessage = "MalformedObjectNameException while createTopicViewMBean: " + e.getMessage();
            logger.error(errorMessage);
            logger.debug(errorMessage + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
            throw new ExportStagingException(e);
        }
        return brokerViewMBean;
    }

    private QueueViewMBean getQueueViewMbean(String queueName) {
        if (!queueViewMBeanList.containsKey(queueName)) {
            //The following for-loop fetches info about the queues available on the Broker.
            for (ObjectName queue : brokerViewMBean.getQueues()) {
                if (queue.getKeyProperty(CONST_DESTINATION_NAME).equalsIgnoreCase(queueName)) {
                    QueueViewMBean queueViewMBean = MBeanServerInvocationHandler.newProxyInstance(connection, queue, QueueViewMBean.class, true);
                    queueViewMBeanList.put(queueName, queueViewMBean);
                    return queueViewMBean;
                }
            }
            logger.info("Queue " + queueName + " does not exists");
        }
        return queueViewMBeanList.get(queueName);
    }

    /**
     * Removes messages from the destination based on an SQL-92 selection on the message headers
     *
     * @param queueName Name of the destination queue
     * @param itemTypes Item types for removing the messages
     * @throws ExportStagingException if there is any problem while removing messages from queue
     */
    public void removeMessagesUsingSelector(String queueName, List<String> itemTypes) throws Exception {
        int removed = getQueueViewMbean(queueName).removeMatchingMessages(prepareSelector(itemTypes));
        String message = "[Subscriber]: Messages removed from queue " + queueName + " for item types" + itemTypes.toString() + ". Total messages removed = " + removed;
        if (removed > 0) {
            logger.info(message);
            System.out.println(message);
        }
    }

    /**
     * Returns the percentage of store limit for each producer
     *
     * @param producerName name of the producer like MasterProducer, ProjectProducer
     * @return Percentage of memory used
     */
    public int getStorePercentUsage(String producerName) throws ExportStagingException {
        int storagePercentSize = 0;
        String producerType = "Core";
        int storageSize = coreQueuesStorageSize;
        if (producerName.equals(projectProducer)) {
            producerType = "Project";
            storageSize = projectQueuesStorageSize;
        }
        try {
            PersistenceAdapterViewMBean adapterViewMBean = getPersistenceAdapterViewMBean(producerName, producerType);
            if (adapterViewMBean != null) {
                long size = adapterViewMBean.getSize();
                double gb = size / bytesToGbDivisor;
                storagePercentSize = (int) ((gb / storageSize) * 100);
            }
        } catch (UndeclaredThrowableException e) {
            persistenceAdapterViewMBeanMap.remove(producerName);
            logger.debug("Persistence storage in kahadb is not exist or may not require for the " + producerType + " queue");
        } catch (ConnectException e) {
            createConnection();
        } catch (Exception e) {
            String message = "Exception while getStorePercentUsage: " + e.getMessage();
            logger.error(message);
            logger.debug(message + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
            persistenceAdapterViewMBeanMap.remove(producerName);
        }
        return storagePercentSize;
    }

    public boolean removeQueueViewMBeanFromList(String queueName) {
        if (queueViewMBeanList.containsKey(queueName)) {
            queueViewMBeanList.remove(queueName);
            return true;
        }
        return false;
    }

    private String prepareSelector(List<String> itemTypes) {
        String selector = "";
        Iterator iterator = itemTypes.iterator();
        while (iterator.hasNext()) {
            selector += jmsTypeSelector + "=\'" + iterator.next() + "\'";
            if (iterator.hasNext()) {
                selector += " OR ";
            }
        }
        return selector;
    }

    /**
     * Prepares JMX url. If hostName contains port then the string before ':' is extracted as a hostname
     *
     * @return String JMX url
     */
    private String getJmxUrl() {
        String host = hostName;
        if (host.contains(":"))
            host = host.substring(0, host.indexOf(":"));
        return "service:jmx:rmi:///jndi/rmi://" + host + ":" + jmxPort + "/jmxrmi";
    }

    /**
     * Returns PersistenceAdapterViewMBean object based on passed parameters
     *
     * @param producerName name of the producer like MasterProducer, ProjectProducer
     * @param producerType type of the producer like Core, Project
     * @return persistenceAdapterView object
     * @throws Exception if unable to create the object
     */
    private PersistenceAdapterViewMBean getPersistenceAdapterViewMBean(String producerName, String producerType) throws Exception {
        PersistenceAdapterViewMBean persistenceAdapterView = persistenceAdapterViewMBeanMap.get(producerName);
        if (persistenceAdapterView == null) {
            ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName + ",service=PersistenceAdapter,instanceName=*");
            Set<ObjectInstance> objectInstances = connection.queryMBeans(objectName, null);
            for (ObjectInstance instance : objectInstances) {
                if (instance.getClassName().equals(PersistenceAdapterView.class.getName()) && instance.getObjectName().toString().toLowerCase().contains(producerType.toLowerCase())) {
                    persistenceAdapterView = MBeanServerInvocationHandler.newProxyInstance(connection, instance.getObjectName(), PersistenceAdapterViewMBean.class, true);
                    persistenceAdapterViewMBeanMap.put(producerName, persistenceAdapterView);
                    break;
                }
            }
        }
        return persistenceAdapterView;
    }
}