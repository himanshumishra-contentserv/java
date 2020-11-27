package com.exportstaging.dataprovider;

import com.exportstaging.abstractclasses.AbstractDataProvider;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.apache.logging.log4j.ThreadContext;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("dataProviderMapping")
public class DataProviderMapping extends AbstractDataProvider {
    @Value("${cassandra.suffix.mapping}")
    private String sSuffixMapping;
    @Value("${cassandra.suffix.view}")
    private String sSuffixView;
    @Value("${cassandra.table.mapping.classid}")
    private String columnClassID;
    @Value("${cassandra.table.mapping.attributeid}")
    private String columnAttributeID;

    private String tableName;

    @Override
    public String getTableName(String itemType) {
        String sItemType = itemType.toLowerCase();
        if (sItemType.contains(sSuffixView)) {
            sItemType = sItemType.substring(0, sItemType.length() - 9);
        }
        return prefixExport + "_" + sItemType + "_" + sSuffixMapping;
    }

    @Override
    public void createTableFromHeaders(String itemType, JSONObject dataModel) throws ExportStagingException {
        tableName = getTableName(itemType);
        ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
        try {
            String createQuery = "CREATE TABLE " + keyspaceTemplate + "." + tableName + "("
                    + "\"ClassID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", \"AttributeID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", PRIMARY KEY(\"ClassID\",\"AttributeID\"));";
            cassandraDAO.dbOperations(createQuery);
            logger.info(tableName + " Table created successfully");
        } catch (Exception e) {
            logError("Exception while creating table.", e);
        } finally {
            ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
        }
    }

    @Override
    public boolean deleteRow(String itemType, String itemID) {
        tableName = getTableName(itemType);
        String deleteQuery;
        try {
            ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
            deleteQuery = "DELETE FROM " + keyspaceTemplate + "." + tableName + " WHERE \"" + columnClassID + "\" = " + itemID;
            cassandraDAO.dbOperations(deleteQuery);
            logger.info("Mapping information successfully deleted for class with ID " + itemID);
        } catch (Exception e) {
            logError("Exception while deleting row with ID: " + itemID, e);
            return false;
        } finally {
            ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
        }
        return true;
    }

    @Override
    public boolean deleteRow(String itemType, String itemID, String sAffectedItemType) {
        return false;
    }

    @Override
    public String getHeaderKey(String dataType) {
        return "";
    }
}
