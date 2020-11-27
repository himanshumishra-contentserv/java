package com.exportstaging.dataprovider;

import com.exportstaging.abstractclasses.AbstractDataProvider;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.apache.logging.log4j.ThreadContext;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component("dataProviderReference")
public class DataProviderReference extends AbstractDataProvider {
    @Value("${cassandra.suffix.reference}")
    private String sSuffixReference;
    @Value("${json.key.reference}")
    private String sReferenceHeaders;

    private String tableName;

    @Override
    public String getTableName(String itemType) {
        return prefixExport + "_" + sSuffixReference;
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
                if (field.equals("AttributeID") || field.equals("ItemID") || field.equals("CSReferenceID") ||
                        field.equals("LanguageID") || field.equals("SourceType")) {
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
                    + ", \"SourceType\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT
                    + ", \"AttributeID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", \"CSReferenceID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", \"LanguageID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", PRIMARY KEY(\"ItemID\",\"SourceType\",\"LanguageID\",\"AttributeID\",\"CSReferenceID\")" + columnNames + ");";
            cassandraDAO.dbOperations(createQuery);
            logger.info(" Table created successfully with name: " + tableName);
        } catch (Exception exception) {
            logError("Exception while creating table with name: " + tableName, exception);
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
                    + " WHERE \"ItemID\" IN (" + message +
                    ") AND \"SourceType\" = '" + itemType + "'";
            cassandraDAO.dbOperations(deleteQuery);
        } catch (Exception e) {
            logger.info("[" + masterSubscriber + "] Data not deleted for Reference with ID(s): " + message);
            logError("Exception while deleting row with ID(s): " + message, e);
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
        return dataType;
    }
}
