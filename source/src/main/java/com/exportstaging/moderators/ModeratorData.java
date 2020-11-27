package com.exportstaging.moderators;

import com.exportstaging.connectors.idbconnector.IntermediateDAO;
import com.exportstaging.dataprovider.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.exportstaging.common.ExportMiscellaneousUtils.TAB_DELIMITER;

@Component("moderatorData")
public class ModeratorData {

    protected final static String LOGGER_NAME = "exportstaging";
    protected final static Logger logger = LogManager.getLogger(LOGGER_NAME);
    protected final static String ITEMTYPE_FILE = "Mamfile";
    protected static final String CONSTANT_TABLENAME = "TableName";
    protected static final String CONSTANT_CORRELATIONID = "CorrelationID";
    protected static final String CONSTANT_PRIORITY = "Priority";
    protected final static String CONSTANT_STRUCTURE = "structure";
    @Autowired
    public DataProviderItem dataProviderItem;
    @Autowired
    protected DataProviderMapping dataProviderMapping;
    @Autowired
    protected DataProviderConfiguration dataProviderConfiguration;
    @Autowired
    protected DataProviderReference dataProviderReference;
    @Autowired
    protected DataProviderSubtable dataProviderSubtable;
    @Autowired
    protected IntermediateDAO intermediateDAO;

    @Value("${core.project.name}")
    protected String projectName;
    @Value("${export.database.name}")
    protected String exportDatabaseName;
    @Value("${mysql.prefix.exportstaging}")
    protected String mPrefixExportStaging;
    @Value("${mysql.csdbprefix.name}")
    protected String mCsDbPrefix;
    @Value("${mysql.type.mapping}")
    protected String mTypeMapping;
    @Value("${mysql.type.item}")
    protected String mTypeItem;
    @Value("${mysql.type.configuration}")
    protected String mTypeConfiguration;
    @Value("${mysql.type.filecontent}")
    protected String mTypeFileContent;
    @Value("${mysql.column.id}")
    protected String columnID;
    @Value("${mysql.column.message}")
    protected String columnMessage;
    @Value("${mysql.column.action}")
    protected String columnAction;
    @Value("${mysql.column.jobid}")
    protected String columnJobID;
    @Value("${mysql.column.jmspriority}")
    protected String columnJmsPriority;
    @Value("${mysql.column.inserttime}")
    protected String columnInsertTime;
    @Value("${mysql.column.versionnr}")
    protected String columnVersionNr;
    @Value("${mysql.column.producerstatus}")
    protected String columnProducerStatus;
    @Value("${mysql.result.batchsize}")
    protected int mResultBatchSize;
    @Value("${mysql.action.failed}")
    protected int mActionFailed;
    @Value("${mysql.action.update}")
    protected int mActionUpdate;
    @Value("${mysql.action.delete}")
    protected int mActionDelete;
    @Value("${mysql.action.initial}")
    protected int mActionInitial;
    @Value("${activemq.message.priority.item}")
    protected int aPriorityItem;
    @Value("${mysql.action.create}")
    protected int mActionCreate;
    @Value("${activemq.message.priority.configuration.mapping}")
    protected int aPriorityConfigurationAndMapping;
    @Value("${activemq.message.priority.filecontent}")
    protected int aPriorityFileContent;
    @Value("${activemq.message.corelationid}")
    protected String aCorrelationID;
    @Value("${activemq.subscriber.elastic}")
    protected String elasticSubscriber;
    @Value("${activemq.subscriber.master}")
    protected String masterSubscriber;
    @Value("${activemq.kahadb.cleanup.wait.time}")
    protected long kahaDbCleanupWaitTime;
    @Value("${activemq.exception.message.storagefull}")
    protected String exceptionMessageStorageFull;

    @Value("${json.key.item}")
    protected String mKeyItem;
    @Value("${json.key.reference}")
    protected String mKeyReference;
    @Value("${json.key.subtable}")
    protected String mKeySubtable;
    @Value("${cassandra.suffix.view}")
    protected String sSuffixView;
    private Map<String, Map<String, Object>> producerSettingMap = new HashMap<>();

    protected Map<String, Object> getProducerSettings(String type, String itemType) {
        if (producerSettingMap.isEmpty()) {
            setProducerSettings(itemType);
        }
        if (producerSettingMap.get(type) == null) {
            return null;
        } else {
            return producerSettingMap.get(type);
        }
    }

    private void setProducerSettings(String itemType) {
        Map<String, Object> list = new HashMap<>();
        list.put(CONSTANT_TABLENAME, getTableName(mTypeItem, itemType));
        list.put(CONSTANT_CORRELATIONID, aCorrelationID);
        list.put(CONSTANT_PRIORITY, aPriorityItem);
        producerSettingMap.put(mTypeItem, list);
        list = new HashMap<>();
        list.put(CONSTANT_TABLENAME, getTableName(mTypeConfiguration, itemType));
        list.put(CONSTANT_CORRELATIONID, masterSubscriber);
        list.put(CONSTANT_PRIORITY, aPriorityConfigurationAndMapping);
        producerSettingMap.put(mTypeConfiguration, list);
        list = new HashMap<>();
        list.put(CONSTANT_TABLENAME, getTableName(mTypeMapping, itemType));
        list.put(CONSTANT_CORRELATIONID, masterSubscriber);
        list.put(CONSTANT_PRIORITY, aPriorityConfigurationAndMapping);
        producerSettingMap.put(mTypeMapping, list);
        if (itemType.equals(ITEMTYPE_FILE)) {
            list = new HashMap<>();
            list.put(CONSTANT_TABLENAME, getTableName(mTypeFileContent, itemType));
            list.put(CONSTANT_CORRELATIONID, elasticSubscriber);
            list.put(CONSTANT_PRIORITY, aPriorityFileContent);
            producerSettingMap.put(mTypeFileContent, list);
        }
    }

    protected String getTableName(String type, String itemType) {
        if (mTypeItem.equals(type)) {
            return mCsDbPrefix + mPrefixExportStaging + "_" + itemType.toLowerCase();
        } else {
            return mCsDbPrefix + mPrefixExportStaging + "_" + itemType.toLowerCase() + "_" + type.toLowerCase();
        }
    }

    protected void logError(String message, Exception e, String component) {
        logger.error("[" + component + "]:" + message + " Error Message:" + e.getMessage());
        logger.debug("[" + component + "]:" + message + " Error Message:" + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
    }
}