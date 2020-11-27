package com.exportstaging.subscribers;

import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.api.exception.ExceptionLogger;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.domain.ConfigurationMessage;
import com.exportstaging.domain.ItemMessage;
import com.exportstaging.domain.Message;
import com.exportstaging.domain.OperationMessage;
import com.exportstaging.initial.DatabaseInitializer;
import com.exportstaging.moderators.PrimeModerator;
import com.exportstaging.utils.CassandraListener;
import org.apache.logging.log4j.LogManager;
import org.json.simple.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.exportstaging.common.ExportMiscellaneousUtils.EXPORT_LOGGER_NAME;

public class MasterSubscriber extends AbstractSubscriber {
    @Autowired
    private PrimeModerator primeModerator;
    @Autowired
    private DatabaseInitializer initializer;
    @Autowired
    private CassandraListener stateListener;


    @Value("${activemq.subscriber.master}")
    private String masterSubscriber;
    @Value("${cassandra.suffix.view}")
    private String sSuffixView;


    @Override
    public void startSubscriber() throws CannotCreateConnectionException
    {
        String databaseType = initializer.getDatabaseType();
        //setSubscriberName(databaseType);
        try {
            logger = LogManager.getLogger(EXPORT_LOGGER_NAME);
            String messageInfo = "-------------------" + databaseType + " Initialization---------------------";
            System.out.println("\n" + messageInfo);
            logger.info(messageInfo);
            Map<String, List<String>> cassandraMVDetails = new HashMap();
            ExportMiscellaneousUtils.getDefaultCassandraMVFields(cassandraMVDetails);
            initializeKeyspace(getHandledItemTypes(), cassandraMVDetails);
            super.startSubscriber();
        } catch (Exception e) {
            logger.error("Error Message :" + e.getMessage() + " Error Trace : " + e.getStackTrace());
        }
    }

    private void initializeKeyspace(
      List<String> itemTypes, Map<String, List<String>> cassandraMVDetails
    ) throws CannotCreateConnectionException {
        if (!initializer.checkKeyspace()) {
            initializer.keyspaceCreator();
        }
        try {
            if (!initializer.checkLanguageTable()) {
                initializer.createLanguageTable();
            }
            if (!initializer.checkReferenceTable()) {
                initializer.createReferenceTable(getHeaders("Reference", "item"));
            }
            for (String itemType : itemTypes) {
                if (ExportMiscellaneousUtils.getConfiguredTypes().contains(itemType)) {
                    if (!initializer.checkItemTable(itemType)) {
                        initializer.createItemTable(itemType, getItemHeader(itemType.toLowerCase()), cassandraMVDetails.get(itemType.toLowerCase()));
                    }
                    if (ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType) && !itemType.contains(sSuffixView)) {
                        if (!initializer.checkItemSubtableTable(itemType)) {
                            initializer.createItemSubtable(itemType, getHeaders(itemType.toLowerCase(), "subtable"));
                        }
                        if (!initializer.checkItemConfigurationTable(itemType)) {
                            initializer.createConfigurationTable(itemType, getHeaders(itemType.toLowerCase(), "configuration"));
                        }
                        if (!initializer.checkItemMappingTable(itemType)) {
                            initializer.createMappingTable(itemType);
                        }
                    }
                }
            }
        } catch (ExportStagingException e) {
            ExceptionLogger.logError("Exception while verifying data model.", e, masterSubscriber);
        }
    }

    @Override
    public boolean isSubscriberReadyForNewMessage() {

        return stateListener.isHostAvailable();
    }

    @Override
    public List<String> getHandledItemTypes() {
        return new ArrayList<>(ExportMiscellaneousUtils.getConfiguredTypes());
    }

    @Override
    public boolean operationCleanup() {
        return initializer.dropKeyspace();
    }

    @Override
    public boolean removedItemTypes(List<String> itemTypes) {
        return initializer.removeItemTypes(itemTypes);
    }

    @Override
    public boolean operationInitialize(List<String> itemTypes, OperationMessage operationMessage) {
        try {
            initializeKeyspace(itemTypes, operationMessage.getCassandraMVDetails());
        } catch (CannotCreateConnectionException e) {
            return false;
        }
        return true;
    }

    @Override
    public Boolean processMessage(ConfigurationMessage configurationMessage, String itemType, String type, String projectName) {
        return primeModerator.setConfigurations(configurationMessage, itemType, type);
    }

    @Override
    public Boolean processMessage(ItemMessage itemMessage, String itemType, String type, String projectName) {
        return primeModerator.setItem(itemMessage, itemType, type);
    }

    @Override
    public Boolean processMessage(Message message, String itemType, String type, String projectName) {
        return primeModerator.setItem(message, itemType, type);
    }

    @Override
    public Boolean deleteMessage(List<String> ids, String itemType, String type, String projectName) {
        return primeModerator.deleteItem(ids, itemType, type);
    }

    @Override
    public void onBeforeShutdown() {
        initializer.closeConnection();
    }
    
    @Override
    public void removeDynamicAttributes(JSONArray array, String itemType) {
        logger.debug("Not removing dynamic attribute for {}", this.getClass());
        return;
    }
}
