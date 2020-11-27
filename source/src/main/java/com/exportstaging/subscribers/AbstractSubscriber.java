package com.exportstaging.subscribers;

import com.exportstaging.activemq.ExportActiveMQUtils;
import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.api.exception.ExceptionLogger;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.cleanup.ActiveMQCleanupThread;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.connectors.idbconnector.IntermediateDAO;
import com.exportstaging.connectors.messagingqueue.ActiveMQMBeanConnection;
import com.exportstaging.connectors.messagingqueue.ActiveMQSpringConnection;
import com.exportstaging.domain.ConfigurationMessage;
import com.exportstaging.domain.ExportMessage;
import com.exportstaging.domain.Message;
import com.exportstaging.domain.ItemMessage;
import com.exportstaging.domain.OperationMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.util.*;

import static com.exportstaging.common.ExportMiscellaneousUtils.*;

@Component
@Configurable
public abstract class AbstractSubscriber implements Subscriber, MessageListener {

    @Autowired
    protected IntermediateDAO intermediateDAO;

    @Autowired
    private DataManager dataManager;
    @Autowired
    private ActiveMQSpringConnection activeMQSpringConnection;
    @Autowired
    private ExportActiveMQUtils exportActiveMQUtils;
    @Autowired
    private ActiveMQMBeanConnection mBeanConnection;
    @Autowired
    private ClassPathXmlApplicationContext applicationContext;

    @Value("${mysql.type.filecontent}")
    String typeFileContent;
    @Value("${mysql.type.configuration}")
    private String typeConfiguration;
    @Value("${mysql.type.mapping}")
    private String typeMapping;
    @Value("${mysql.type.item}")
    String typeItem;
    @Value("${json.key.record}")
    private String typeRecord;
    @Value("${activemq.virtual.topic.core}")
    private String exportCoreVirtualTopic;
    @Value("${activemq.virtual.topic.project}")
    private String exportProjectVirtualTopic;
    @Value("${activemq.message.corelationid}")
    private String correlationID;
    @Value("${mysql.column.id}")
    private String ID;
    @Value("${mysql.column.message}")
    private String message;
    @Value("${mysql.column.action}")
    private String action;
    @Value("${mysql.prefix.exportstaging}")
    private String idbTablePrefix;
    @Value("${mysql.column.jobid}")
    private String jobId;
    @Value("${mysql.action.create}")
    protected int actionCreate;
    @Value("${mysql.action.update}")
    private int actionUpdate;
    @Value("${mysql.action.delete}")
    private int actionDelete;
    @Value("${core.project.name}")
    protected String projectName;
    @Value("${mysql.csdbprefix.name}")
    protected String mCsDbPrefix;
    @Value("${activemq.message.selector.jmstype}")
    private String jmsTypeSelector;
    @Value("${activemq.message.priority.item}")
    private int jmsPriorityItem;
    @Value("${activemq.message.priority.configuration.mapping}")
    private int jmsPriorityConfigurationMapping;
    @Value("${export.database.name}")
    private String exportDatabaseName;
    /**
     * Variable getting from admin side core properties
     */
    @Value(("${export.supported.item.types}"))
    String exportSupportedItemTypes;
    @Value(("${export.supported.record.types}"))
    String exportSupportedRecordTypes;

    private List<String> exportDbNames = new ArrayList<>();
    private Map<String, Session> sessions = new HashMap<>();
    private Map<String, MessageConsumer> consumers = new HashMap<>();
    private String subscriberEquivalentName;


    public void setSubscriberName(String subscriberName) {
        this.subscriberName = subscriberName;
    }

    String subscriberName;

    private String[] logTemplates = {"#SubscriberName", "#operation", "#itemType", "#type", "#id", "#tookTime"};

    private List<String> supportedItemTypes;

    protected static Logger logger;

    public void startSubscriber() throws CannotCreateConnectionException {
        logger = LogManager.getLogger(EXPORT_LOGGER_NAME);
        subscriberName = getSubscriberName();
        log("initializing...");
        supportedItemTypes = convertFirstLetterToUpperCase(getHandledItemTypes());
        if (isCustomSubscriber() && (supportedItemTypes == null || supportedItemTypes.size() == 0)) {
            String msg = "[" + subscriberName + "]: Provide object types to start the subscriber";
            System.out.println(msg);
            logger.info(msg);
            return;
        }
        supportedItemTypes.add(EXPORT_TYPE_OPERATION);
        intermediateDatabaseMaintenance();

        if (exportDbNames.isEmpty()) {
            exportDbNames.add(exportDatabaseName);
        }
        createConsumer(supportedItemTypes);
        subscriberEquivalentName = isCustomSubscriber() ? subscriberName : "coreSubscriber";

        if (!this.isHandlingInitialExport()) {
            log("ignores initial export messages");
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownCleaner());
    }

    private void intermediateDatabaseMaintenance() {
        //TODO: Refactor! Pass the current ItemTypes to IDB and delete query based on the Types that are not available in current ItemTypes for this subscriber.
        intermediateDAO.createTableForItemType(supportedItemTypes);
        List<String> removedItemTypes = intermediateDAO.getRemovedItemTypes(subscriberName, supportedItemTypes);

        if (removedItemTypes.size() > 0) {
            try {
                intermediateDAO.removeEntriesFromSubscriberMappingTable(subscriberName, removedItemTypes);
            } catch (ExportStagingException e) {
                ExceptionLogger.logError("Exception while removeEntriesFromSubscriberMappingTable.", e, subscriberName);
            }
            //TODO: Remove entries from DataManager for CustomSubscriber
        }
        intermediateDAO.insertSubscriberItemTypeMapping(subscriberName, supportedItemTypes, isCustomSubscriber());
    }

  /**
   * This method is called whenever a message is received from activemq.
   * The message gets transformed into a domain object and then is passed to the corresponding processMessage(...)
   * for subscriber side processing.
   */
  public void onMessage(javax.jms.Message message)
  {
    Session session     = sessions.get(exportDatabaseName);
    Boolean acknowledge = false;
    String type, itemType, itemID = null;
    try {
      if (!isSubscriberReadyForNewMessage()) {
        logger.debug(
          "[" + subscriberName + "] is not ready to pickup new messages. Initial activity is going on, once it is " +
          "done message will process.");
        session.recover();
        return;
      }

      if (!supportedItemTypes.contains(message.getJMSType())) {
        message.acknowledge();
        return;
      }

      long       startTime             = System.currentTimeMillis();
      String     jmsCorrelationId      = message.getJMSCorrelationID();
      MapMessage mapMessage            = (MapMessage) message;
      int        action                = mapMessage.getInt(this.action);
      String     queueName             = message.getJMSDestination().toString();
      String     exportDbnameFromQueue = queueName.substring(queueName.lastIndexOf(".") + 1);
      String     projectName           = ExportMiscellaneousUtils.getProjectDetails().get(exportDbnameFromQueue);

      if (jmsCorrelationId == null || !(jmsCorrelationId.contains(correlationID) || jmsCorrelationId.contains(
        subscriberName))) {
        message.acknowledge();
        return;
      }
      else if (typeItem.equalsIgnoreCase(EXPORT_ITEM_TYPE_WORKFLOW) && action != actionDelete) {
        message.acknowledge();
        return;
      }

      int priority = message.getJMSPriority();

      // if subscriber is not handling initial exports we acknowledge message without forwarding to subscriber
      if (((priority == jmsPriorityItem) || (priority == jmsPriorityConfigurationMapping)) && !this
        .isHandlingInitialExport()) {
        message.acknowledge();
        return;
      }

      int  jobID      = mapMessage.getInt(this.jobId);
      long insertTime = mapMessage.getLong(IDB_INSERT_TIME);

      type     = mapMessage.getString(CONSTANT_TYPE);
      itemID   = mapMessage.getString(ID);
      itemType = mapMessage.getString(CONSTANT_ITEM_TYPE);

      String exportData = mapMessage.getString(this.message);

      if (!EXPORT_TYPE_OPERATION.equalsIgnoreCase(itemType)) {
        long l1 = System.currentTimeMillis();
        exportData = ExportMiscellaneousUtils.getUnCompressedItemMessage(exportData);
        logger.debug(
          "Total time for decompression of " + subscriberName
          + " " + itemType
          + " " + itemID + " is "
          + (System.currentTimeMillis() - l1)
        );
      }
      int jmsPriority = mapMessage.getJMSPriority();

      ThreadContext.put(EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
      ThreadContext.put(EXPORT_DATABASE_LOG_OPERATION_TYPE, getOperationType(action, jmsPriority, type));
      ThreadContext.put(EXPORT_DATABASE_LOG_TRACE_ID, String.valueOf(insertTime));

      ExportMessage exportMessage = new ExportMessage(itemID, exportData, action, itemType, type, jobID);

      if (itemType.equalsIgnoreCase(EXPORT_TYPE_OPERATION)) {
        acknowledge = handleOperationMessage(insertTime, exportMessage.getOperationData());
      }
      else {
        if (!type.equals(typeConfiguration) && !type.equals(typeMapping)) {
        }
        if ((type.equals(typeItem) || type.equals(typeFileContent)) && getCoreItemTypes().contains(itemType)) {
          if (!isMessageValid(mapMessage)) {
            logger.warn("[" + subscriberName + "] Message with ID " + itemID + " is no longer valid");
            message.acknowledge();
            return;
          }
        }
        try {
          acknowledge = handleDataMessage(startTime, projectName, itemID, itemType, type, exportMessage, jmsPriority,
                                          insertTime);
        } catch (Exception e) {
          ExceptionLogger.logError("Framework exception on processing message for itemType:" + itemType + ", ItemID:" + itemID, e,
                   subscriberName);
        }
      }
    } catch (JMSException e) {
      handleJmsException(e, session, message, acknowledge);
    }  catch (ExportStagingException e) {
      ExceptionLogger.logError("Export staging exception while getting project name detail", e, subscriberName);
    } finally {
      try {
      handleAcknowledgement(message, session, acknowledge);
      } catch (JMSException e) {
        handleJmsException(e, session, message, acknowledge);
      }
      if (!acknowledge && isDurableSubscriber()) {
        logger.warn("[" + subscriberName + "] Recovering message with ID: " + itemID + " and Action: " + action);
      }
      ThreadContext.clearAll();
    }
  }


  private void handleAcknowledgement(javax.jms.Message message, Session session, Boolean acknowledge)
    throws JMSException
  {
    if (isDurableSubscriber()) {
      if (acknowledge) {
        message.acknowledge();
      }
      else {
        session.recover();
      }
    }
    else {
      message.acknowledge();
    }
  }

    private void handleJmsException(JMSException jmsException, Session session, javax.jms.Message message, boolean acknowledge) {
        Throwable cause = jmsException.getCause();
        if (cause instanceof InterruptedException || (Thread.currentThread().isInterrupted())) {
            Thread.currentThread().interrupt();
        } else {
            ExceptionLogger.logError("JMSException ", jmsException, subscriberName);
            try {
                handleAcknowledgement(message, session, acknowledge);
            } catch (JMSException e) {
                handleJmsException(e, session, message, acknowledge);
            }
        }
    }

    /**
     * Unregisters the subscriber from activemq.
     */
    final public void unregisterSubscriber() {
        closeSessions(true);
        logger.info("[" + subscriberName + "] : Unregistered");
    }

    public abstract List<String> getHandledItemTypes();

    @Deprecated
    public void processMessage(ConfigurationMessage configurationMessage, String itemType, String type) {
    }

    /**
     * Configuration related messages are passed to the corresponding subscriber using this method.
     * This is a Empty method and needs the corresponding subscriber to override it and write business logic
     *
     * @param configurationMessage {@link ConfigurationMessage} object which has data accessors for Attribute/Classes.
     * @param itemType             ItemType of the corresponding message
     * @param type                 Defines if the message is a configuration or mapping
     * @param projectName          Contains the project name the message belongs to
     * @return true to acknowledge for the configuration message to JMS.
     */
    public Boolean processMessage(ConfigurationMessage configurationMessage, String itemType, String type, String projectName) {
        return true;
    }

    @Deprecated
    public void processMessage(ItemMessage itemMessage, String itemType, String type) {
    }

    /**
     * Item related messages are passed to the underlying subscriber using this method.
     * This is a Empty method and needs the corresponding subscriber to override it and write business logic
     *
     * @param itemMessage {@link ItemMessage} object which has data accessors for Item.
     * @param itemType    ItemType of the corresponding message
     * @param type        Defines the type of message
     * @param projectName Contains the project name the message belongs to
     * @return true to acknowledge for the ItemMessage to JMS.
     */
    public Boolean processMessage(ItemMessage itemMessage, String itemType, String type, String projectName) {
        return true;
    }

    @Deprecated
    public void processMessage(Message message, String itemType, String type) {
    }

    /**
     * Item related messages are passed to the underlying subscriber using this method.
     * This is a Empty method and needs the corresponding subscriber to override it and write business logic
     *
     * @param message     {@link com.exportstaging.domain.Message} object which has data accessors for Records.
     * @param itemType    ItemType of the corresponding message
     * @param type        Defined the type of message
     * @param projectName Contains the project name the message belongs to
     * @return true to acknowledge for the domain.Message to JMS.
     */
    public Boolean processMessage(Message message, String itemType, String type, String projectName) {
        return true;
    }

    @Deprecated
    public void deleteMessage(List<String> ids, String itemType, String type) {
    }

    /**
     * Provides information of the deleted messages for all Item/Record/Configuration types
     *
     * @param ids         {@link List} of Deleted IDs
     * @param itemType    ItemType of the deleted object
     * @param type        Defines the type of the message
     * @param projectName Contains the project name the message belongs to
     * @return true to acknowledge for the deleteMessage to JMS.
     */
    public Boolean deleteMessage(List<String> ids, String itemType, String type, String projectName) {
        return true;
    }

    /**
     * This method is called when an ItemType is removed from CS-ESA UI. The Subscriber can perform any cleanup tasks for the removed itemTypes if required.
     *
     * @param itemTypes List of Removed ItemTypes
     * @return If respective tasks to be performed are complete, then return true.
     */
    public boolean removedItemTypes(List<String> itemTypes) {

        return true;
    }

    /**
     * ExportDatabase Operation which informs the CustomSubscriber to perform the initialization activity
     *
     * @param itemTypes List Of itemTypes which are added
     * @return Should return true if the initialization activity is performed properly, else false
     */
    public boolean operationInitialize(List<String> itemTypes) {
        return true;
    }


  /**
   * ExportDatabase Operation which informs the Subscriber to perform the initialization activity
   *
   * @param itemTypes list of item types
   * @param operationMessage OperationMessage contains details of provided operation
   *                         in case of mastersubscriber(cassandra subscriber) operation message will provide list of
   *                         fields on which MV should be created.
   *
   * @return Should return true if the initialization activity is performed properly, else false
   */
    public boolean operationInitialize(List<String> itemTypes, OperationMessage operationMessage){
      return operationInitialize(itemTypes);
    }


    /**
     * ExportDatabase Operation which informs the CustomSubscriber to perform the cleanup activity
     *
     * @return Should return true if the cleanup activity is performed properly, else false
     */
    public boolean operationCleanup() {
        return true;
    }

    /**
     * ExportDatabase logger debug mode which informs the CustomSubscriber to switch the debug mode
     *
     * @param level Logger debug mode
     * @return Should return true if the logger debug mode activity is performed properly, else false
     */
    public boolean loggerDebugMode(String level) {
        return true;
    }

    public void onBeforeShutdown() {

    }

    public boolean isSubscriberReadyForNewMessage() {
        return true;
    }

    /**
     * Method will set the export database name
     *
     * @param exportDbNames List of export database names
     */
    final void setExportDbNames(List<String> exportDbNames) {
        this.exportDbNames = exportDbNames;
    }

    /**
     * Will provide the export databases names
     *
     * @return List of export database names
     */
    final protected List<String> getExportDbNames() {
        return this.exportDbNames;
    }

    /**
     * Can be overwritten to tell the framework that subscriber is not durable. This means that
     * activemq will not preserve messages if the subscriber is closed in any fassion.
     *
     * @return Boolean    true if subscriber should be durable (default), otherwise false
     */
    protected Boolean isDurableSubscriber() {
        return true;
    }

    /**
     * Defines whether the current subscriber wants to listen to inital export messages.
     *
     * @return Boolean      true, if initial export messages should be delivered, otherwise false
     */
    protected Boolean isHandlingInitialExport() {
        return true;
    }

    protected boolean isCustomSubscriber() {
        return false;
    }

    private void removeMessagesFromQueue(List<String> removedItemTypes) {
        String queueName = activeMQSpringConnection.getQueueName(projectName, subscriberName, isCustomSubscriber());
        logger.info("[" + getSubscriberName() + "]: Start the performing cleanup of messages on " + queueName + " for item types: " + removedItemTypes.toString());
        ActiveMQCleanupThread activeMQCleanup = (ActiveMQCleanupThread) applicationContext.getBean("activeMQCleanupThread");
        activeMQCleanup.setQueueName(queueName);
        activeMQCleanup.setItemTypes(removedItemTypes);
        activeMQCleanup.start();
    }

    private void createConsumer(List<String> itemTypes) {
        for (String exportDbName : exportDbNames) {
            try {
                Session session = getSession(exportDbName);
                MessageConsumer consumer = activeMQSpringConnection.getConsumer(exportDbName, subscriberName, session, itemTypes, isDurableSubscriber(), isCustomSubscriber());
                consumer.setMessageListener(this);
                logSubscriberIsDurableOrNot();
                consumers.put(exportDbName, consumer);
            } catch (JMSException e) {
                ExceptionLogger.logError("Exception while creating consumer.", e, subscriberName);
            }
        }
    }

    private boolean handleOperationMessage(long insertTime, OperationMessage operationMessage) {
        List<String> itemTypes = operationMessage.getItemTypes();
      boolean status = false;
        switch (operationMessage.getOperationType()) {
            case OperationMessage.OPERATION_INITIALIZE:
                status = operationInitialize(insertTime, itemTypes, operationMessage);
                break;
            case OperationMessage.OPERATION_ITEM_TYPE_ADDED:
                status = operationTypeAdded(itemTypes, operationMessage);
                intermediateDatabaseMaintenance();
                break;
            case OperationMessage.OPERATION_ITEM_TYPE_REMOVED:
                status = operationTypeRemoved(itemTypes);
                intermediateDatabaseMaintenance();
                break;
            case EXPORT_JSON_KEY_DEBUG_MODE:
                status = loggerDebugMode(itemTypes.get(0));
                break;
            case EXPORT_JSON_KEY_REINDEX:
                if (subscriberName.equalsIgnoreCase("SearchableElasticSubscriber")) {
                  status = operationTypeReindex(operationMessage, intermediateDAO.getLanguageIds());
                } else {
                    status =true;
                }
        }
        return status;
    }

    private boolean isMessageValid(MapMessage mapMessage) {
        dataManager.updateData(mapMessage, subscriberEquivalentName);
        return dataManager.isMessageValid(mapMessage);
    }

    private Boolean handleDataMessage(long startTime, String projectName, String itemID, String itemType, String type, ExportMessage exportMessage, int jmsPriority, long idbInsertTime) {
        Boolean acknowledge = false;
        String operation;
        long totalTime;
        int action = exportMessage.getAction();
        if (action == actionDelete) {
            operation = "delete";
            acknowledge = handleDeletedMessages(projectName, itemID, itemType, type, exportMessage);
        } else {
            operation = "update";
            String messageType = exportMessage.getMessageType();
            if (type.equals(typeItem) || type.equals(typeFileContent)) {
                //TODO: Should we have different API for fileContent?
                if (messageType.equals(typeItem)) {
                    ItemMessage itemMessage = exportMessage.getItemMessage();
                    this.removeDynamicAttributes(itemMessage.getParsedItem(), itemType);
                    acknowledge = handleItemUpdates(projectName, itemType, type, itemMessage);
                } else if (messageType.equals(typeRecord)) {
                    acknowledge = handleRecordUpdates(projectName, itemType, type, exportMessage.getMessageData());
                }
            } else if (type.equals(typeConfiguration) || type.equals(typeMapping)) {
                acknowledge = handleConfigurationUpdates(projectName, itemType, type, exportMessage.getConfigurationMessage());
            }
        }
        totalTime = System.currentTimeMillis() - startTime;

        log(operation, itemType, type, itemID, totalTime, acknowledge, action, jmsPriority, idbInsertTime);
        return acknowledge;
    }

    private boolean operationTypeRemoved(List<String> itemTypes) {
        boolean status = false;
        if (!isCustomSubscriber()) {
            refreshItemTypeCache();
            supportedItemTypes.removeAll(itemTypes);
            status = removedItemTypes(itemTypes);
            dataManager.removeItemType(itemTypes);
        }
        return status;
    }


    private boolean operationTypeAdded(List<String> itemTypes, OperationMessage operationMessage) {
        boolean status = false;
        if (!isCustomSubscriber()) {
            refreshItemTypeCache();
            supportedItemTypes.addAll(itemTypes);
            status = operationInitialize(itemTypes, operationMessage);
        }
        return status;
    }


    /**
     * Method overridden in ElasticSubscriber
     *
     * @param operationMessage Operation message received for reindex
     * @return
     */
    public boolean operationTypeReindex(OperationMessage operationMessage, List<String> languageIds) {
        return true;
    }

    private boolean operationInitialize(long insertTime, List<String> itemTypes, OperationMessage operationMessage) {
        boolean status;
        refreshItemTypeCache();
        status = operationCleanup();
        if (status) {
            dataManager.clearMap(insertTime, subscriberEquivalentName);
            if ((status = operationInitialize(itemTypes, operationMessage)) && !isCustomSubscriber()) {
                dataManager.loadMap(subscriberEquivalentName);
            }
        }
        return status;
    }

    private Boolean handleConfigurationUpdates(String projectName, String itemType, String type, ConfigurationMessage configurationMessage) {
        processMessage(configurationMessage, itemType, type);
        return processMessage(configurationMessage, itemType, type, projectName);
    }

    private Boolean handleItemUpdates(String projectName, String itemType, String type, ItemMessage itemMessage) {
        processMessage(itemMessage, itemType, type);
        return processMessage(itemMessage, itemType, type, projectName);
    }

    private Boolean handleRecordUpdates(String projectName, String itemType, String type, Message message) {
        processMessage(message, itemType, type);
        return processMessage(message, itemType, type, projectName);
    }

    private Boolean handleDeletedMessages(String projectName, String itemID, String itemType, String type, ExportMessage exportMessage) {
        List<String> ids;
        if (exportMessage.getMessage().equals("")) {
            ids = new ArrayList<>();
            ids.add(itemID);
        } else {
            ids = new ArrayList<>(Arrays.asList(exportMessage.getMessage().split(",")));
        }
        deleteMessage(ids, itemType, type);
        return deleteMessage(ids, itemType, type, projectName);
    }

    /**
     * Shutdown the subscriber and close all the corresponding connection object.
     */
    private void stopSubscriber() {
        exportDbNames.stream().filter(projectName -> !isDurableSubscriber()).forEachOrdered(projectName -> {
            String queueName = activeMQSpringConnection.getQueueName(projectName, subscriberName, isCustomSubscriber());
            System.out.println(queueName);
            exportActiveMQUtils.deleteQueue(queueName);
            //Removing item type entries from mapping table
            try {
                List<String> removedItemTypes = intermediateDAO.getItemTypesFromMappingTable(subscriberName);
                intermediateDAO.removeEntriesFromSubscriberMappingTable(subscriberName);
                intermediateDAO.dropIntermediateTableForItemType(removedItemTypes);
            } catch (ExportStagingException e) {
                ExceptionLogger.logError("Exception while removeEntriesFromSubscriberMappingTable.", e, subscriberName);
            }
            if (mBeanConnection.removeQueueViewMBeanFromList(queueName)) {
                logger.info("[" + subscriberName + "] Remove queue " + queueName + " object from maintain list");
            }
        });
        closeSessions(false);
        logger.info("[" + subscriberName + "] : Stopped");
    }

    /**
     * Returns the name of the current subscriber.
     *
     * @return String    Subscriber name
     */
    public String getSubscriberName() {
        if (this.subscriberName == null) {
            if (this.getClass().getPackage() != null) {
                return StringUtils.replace(this.getClass().getName(), this.getClass().getPackage().getName() + ".", "");
            } else {
                return this.getClass().getName();
            }
        }
        return this.subscriberName;
    }

    private void log(String operation, String itemType, String type, String itemID, long totalTime, Boolean success, int action, int jmsPriority, long idbInsertTime) {
        if (!isCustomSubscriber()) {
            ThreadContext.put(EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
            ThreadContext.put(EXPORT_DATABASE_LOG_OPERATION_TYPE, getOperationType(action, jmsPriority, type));
            ThreadContext.put(EXPORT_DATABASE_LOG_TRACE_ID, String.valueOf(idbInsertTime));
            String logEntry;
            type = type.equals("") ? CONSTANT_ITEM_TYPE : type;
            if (success) {
                String logSuccessTemplate = "[#SubscriberName] Successfully #operation data with id #id. Took(ms) #tookTime";
                logEntry = StringUtils.replaceEach(
                        logSuccessTemplate,
                        logTemplates,
                        new String[]{subscriberName, operation + "d", itemType, type, itemID, String.valueOf(totalTime)}
                );
            } else {
                String logFailureTemplate = "[#SubscriberName] Failed to #operation data with id #id. Took(ms) #tookTime";
                logEntry = StringUtils.replaceEach(logFailureTemplate, logTemplates, new
                        String[]{subscriberName, operation, itemType, type, itemID, String.valueOf(totalTime)});
            }
            logger.info(logEntry);
            ThreadContext.clearAll();
        }
    }

    final void log(String text) {
        text = "[" + getSubscriberName() + "] : " + text;
        System.out.println(text);
        logger.info(text);
    }

    private Session getSession(String exportDbName) {
        Session session = sessions.get(exportDbName);
        if (session == null) {
            String clientName = exportProjectVirtualTopic + "_" + exportDbName + "_" + subscriberName;
            session = activeMQSpringConnection.createSession(clientName, isDurableSubscriber());
            sessions.put(exportDbName, session);
            logger.info("[" + subscriberName + "] session created");
        }

        return session;
    }

    private void closeSessions(Boolean unregister) {
        for (String key : sessions.keySet()) {
            Session session = sessions.get(key);
            MessageConsumer consumer = consumers.get(key);
            if (session != null) {
                try {
                    consumer.close();
                    if (unregister) {
                        session.unsubscribe(this.subscriberName + "_" + key);
                    }
                    session.close();
                } catch (JMSException e) {
                    ExceptionLogger.logError("Exception while closing session: ", e, subscriberName);
                }
            }
        }
        sessions.clear();
        activeMQSpringConnection.close();
    }

    private void logSubscriberIsDurableOrNot() {
        String subscriberMessage = "[" + subscriberName + "] : " + "created as ";
        if (isDurableSubscriber()) {
            logger.info(subscriberMessage + "durable subscriber");
            System.out.println(subscriberMessage + "durable subscriber");
        } else {
            logger.info(subscriberMessage + "non-durable subscriber");
            System.out.println(subscriberMessage + "non-durable subscriber");
        }

        subscriberMessage += " (" + projectName + ")";

        logger.info(subscriberMessage);
        System.out.println(subscriberMessage);
    }

    public final JSONObject getHeaders(String itemType, String type) {
        return intermediateDAO.getHeaders(itemType, type);
    }

    public final Map<String, JSONObject> getItemHeaders(List<String> itemTypes) {
        return intermediateDAO.getHeaders(itemTypes);
    }

    public final JSONObject getItemHeader(String itemType) {
        if (itemType.equalsIgnoreCase(EXPORT_ITEM_TYPE_MAMFILECONTENT)) {
            return new JSONObject();
        }

        return intermediateDAO.getHeaders(itemType, "item");
    }

    @Override
    public void removeDynamicAttributes(JSONArray array, String itemType) {
        String dynamicAttributes = ExportMiscellaneousUtils.getPropertyValue(
                ExportMiscellaneousUtils.EXPORT_CONFIGURED_DYNAMIC_ATTRIBUTES + itemType.replace("structure", ""),
                ExportMiscellaneousUtils.CORE_PROPERTIES, projectName);

        if (!StringUtils.isEmpty(dynamicAttributes)) {
            logger.debug("Removing dynamic attributes {} for {}", dynamicAttributes, this.getClass());
            for (String attrId : dynamicAttributes.split(",")) {
                for (Object obj : array) {
                    ((JSONObject) obj).remove(attrId + ":Value");
                    ((JSONObject) obj).remove(attrId + ":FormattedValue");
                }
            }
        }
    }

    private class ShutdownCleaner extends Thread {
        public void run() {
            onBeforeShutdown();
            stopSubscriber();
        }
    }
}
