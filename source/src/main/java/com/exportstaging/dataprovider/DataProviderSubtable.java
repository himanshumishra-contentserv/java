package com.exportstaging.dataprovider;

import com.exportstaging.abstractclasses.AbstractDataProvider;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.apache.logging.log4j.ThreadContext;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component("dataProviderSubtable")
public class DataProviderSubtable extends AbstractDataProvider {
    @Value("${cassandra.suffix.subtable}")
    private String sSuffixSubtable;
    @Value("${cassandra.suffix.view}")
    private String sSuffixView;
    @Value("${json.key.subtable}")
    private String sSubtableeHeaders;

    private String tableName;

    @Override
    public String getTableName(String itemType) {
        String sItemType = itemType.toLowerCase();
        if (sItemType.contains(sSuffixView)) {
            sItemType = sItemType.substring(0, sItemType.length() - 9);
        }
        return prefixExport + "_" + sItemType + "_" + sSuffixSubtable;
    }

    @Override
    public void createTableFromHeaders(String itemType, JSONObject dataModel) throws ExportStagingException {
        tableName = getTableName(itemType);
        String columnNames = "";
        try {
            ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
            Set headerData = dataModel.keySet();
            String columnType;

            for (Object key : headerData) {
                String field = key.toString();
                if (field.equals("AttributeID") || field.equals("ItemID") || field.equals("ItemTableID") ||
                        field.equals("LanguageID") || field.equals("ItemType")) {
                    continue;
                }
                columnType = dataModel.get(field).toString();
                if (columnType.equals("")) {
                    columnType = ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT;
                }
                columnNames += ", \"" + field + "\"" + columnType;
            }
            String createQuery = "CREATE TABLE " + keyspaceTemplate + "." + tableName + "("
                    + "\"ItemID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", \"ItemType\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT
                    + ", \"LanguageID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", \"AttributeID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", \"ItemTableID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", PRIMARY KEY(\"ItemID\", \"ItemType\", \"LanguageID\",\"AttributeID\",\"ItemTableID\")" + columnNames + ");";
            cassandraDAO.dbOperations(createQuery);
            logger.info(tableName + " Table created successfully");
        } catch (Exception exception) {
            logError("Exception while creating table with name:" + tableName, exception);
            throw new ExportStagingException(exception);
        } finally {
            ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
        }
    }

    @Override
    public boolean deleteRow(String itemType, String message) {
        String deleteQuery;
        try {
            ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
            deleteQuery = "DELETE FROM " + keyspaceTemplate + "." + getTableName(itemType)
                    + " WHERE \"ItemID\" IN(" + message
                    + ") AND \"ItemType\" = '" + itemType + "'";
            cassandraDAO.dbOperations(deleteQuery);
        } catch (Exception e) {
            logError("Exception while deleting subtable row for ID(s): " + message, e);
            logger.info("[" + masterSubscriber + "] Data not deleted for subtable with ID(s): " + message);
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
        return (dataType + "_subtable").toLowerCase();
    }
}
