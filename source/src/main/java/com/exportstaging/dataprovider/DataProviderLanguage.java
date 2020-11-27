package com.exportstaging.dataprovider;

import com.exportstaging.abstractclasses.AbstractDataProvider;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.apache.logging.log4j.ThreadContext;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("dataProviderLanguage")
public class DataProviderLanguage extends AbstractDataProvider {
    @Value("${data.json.file.suffix.data}")
    private String sSuffixData;
    @Value("${cassandra.suffix.language}")
    private String sSuffixLanguage;
    @Value("${json.key.language}")
    private String sLanguagenData;

    private String tableName;

    @Override
    public String getTableName(String itemType) {
        return prefixExport + "_" + sSuffixLanguage;
    }

    @Override
    public void createTableFromHeaders(String itemType, JSONObject dataModel) {
        //Note: For language, dataModel is always null. In future, if languageHeaders are created, you will get this in the above parameter
        tableName = getTableName(itemType);
        ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
        try {
            String createQuery = "CREATE TABLE " + keyspaceTemplate + "." + tableName + "("
                    + "\"ID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT
                    + ",\"FullName\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT
                    + ",\"ShortName\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT
                    + ",\"Available\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT
                    + ",\"IsDefault\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT
                    + ",\"IsFolder\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT
                    + ",\"ParentID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT
                    + ",\"_IsCreated\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT
                    + ",\"_LastWritten\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TIMESTAMP
                    + ",\"_InsertTime\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TIMESTAMP
                    + ", PRIMARY KEY(\"ID\"));";
            cassandraDAO.dbOperations(createQuery);
            logger.info(tableName + " table created successfully");
            String sIndexQuery = "CREATE INDEX INDEX_LANGUAGE_IS_DEFAULT ON " + keyspaceTemplate + "." + tableName + "" + "(\"IsDefault\");";
            cassandraDAO.dbOperations(sIndexQuery);
            logger.info("Index created on IsDefault field");
            sIndexQuery = "CREATE INDEX INDEX_LANGUAGE_SHORT_NAME ON " + keyspaceTemplate + "." + tableName + "" + "(\"ShortName\");";
            cassandraDAO.dbOperations(sIndexQuery);
            logger.info("Index created on ShortName field");
        } catch (Exception e) {
            logError("Exception while creating table using headers.", e);
        } finally {
            ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
        }
    }

    @Override
    public boolean deleteRow(String itemType, String itemID) {
        tableName = getTableName(itemType);
        String deleteQuery;
        ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
        try {
            deleteQuery = "DELETE FROM " + keyspaceTemplate + "." + tableName + " WHERE \"ID\" = " + itemID;
            cassandraDAO.dbOperations(deleteQuery);
            logger.info("Data successfully deleted with ID " + itemID);
        } catch (Exception e) {
            logError("Exception while deleting row for ID: " +itemID, e);
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
