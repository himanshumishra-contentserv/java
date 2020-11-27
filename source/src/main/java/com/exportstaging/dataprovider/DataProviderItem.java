package com.exportstaging.dataprovider;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.exportstaging.abstractclasses.AbstractDataProvider;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.apache.logging.log4j.ThreadContext;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Component("dataProviderItem")
public class DataProviderItem extends AbstractDataProvider {
    private String tableName;

    @Override
    public String getTableName(String itemType) {
        return prefixExport + "_" + itemType.toLowerCase();
    }

    private String getMaterializedViewName(String itemType, String columnName) {
        return itemType + "_view_" + columnName;
    }

    public void createMaterializedView(String itemType, List<String> viewFieldName) throws ExportStagingException
    {
        tableName = getTableName(itemType);
        if (viewFieldName != null) {
            for (String fieldName : viewFieldName) {
                if (itemType.equals(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_USER) && (fieldName.equals(
                  ExportMiscellaneousUtils.EXPORT_FIELD_STATEID) || fieldName.equals(
                  ExportMiscellaneousUtils.EXPORT_FIELD_WORKFLOWID))) {
                    continue;
                }

                String viewColumns = "\"LanguageID\", \"" + fieldName + "\", \"ID\"";
                String notNullColumns = "\"LanguageID\" IS NOT NULL AND \"" + fieldName + "\" IS NOT NULL AND \"ID\" " +
                                        "" + "IS NOT" + " NULL";

                if (fieldName.equals(CONST_EXPORT_LANGUAGE_ID)) {
                    viewColumns = "\"LanguageID\", \"ID\"";
                    notNullColumns = "\"LanguageID\" IS NOT NULL AND \"ID\" IS NOT NULL";
                }

                fieldName = fieldName.replace(":", "_");
                String dropExistingView = "DROP MATERIALIZED VIEW IF EXISTS "
                                          + keyspaceTemplate + "." + tableName + "_view_" + fieldName + ";";
                String createView = "CREATE MATERIALIZED VIEW "
                                    + keyspaceTemplate + "." + tableName + "_view_" + fieldName
                                    + " AS SELECT " + viewColumns
                                    + " FROM " + keyspaceTemplate + "." + tableName
                                    + " WHERE " + notNullColumns + " PRIMARY KEY (" + viewColumns + ")"
                                    + "" + " WITH caching = { 'keys' : 'NONE', 'rows_per_partition' : '10000' }"
                                    + " " + "AND " + "comment = '" + itemType + " with " + fieldName + "';";

                cassandraDAO.dbOperations(dropExistingView);
                cassandraDAO.dbOperations(createView);
            }
        }
    }

    @Override
    public void createTableFromHeaders(String itemType, JSONObject dataModel) throws ExportStagingException {
        String createQuery;
        tableName = getTableName(itemType);
        String columnNames = "";
        try {
            String columnType;
            Set headerData = dataModel.keySet();
            for (Object key : headerData) {
                String field = key.toString();
                if (field.equals("ID") || field.equals("LanguageID")) {
                    continue;
                }
                columnType = dataModel.get(field).toString();
                if (columnType.equals("")) {
                    columnType = ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT;
                }
                columnNames += ", \"" + field + "\"" + columnType;
            }
            for (String field : esaDataInDate) {
                columnNames += ", \"" + field + "\"" + getExportDatabaseFieldDataType(field);
            }
            for (String field : esaDataInInt) {
                columnNames += ", \"" + field + "\"" + getExportDatabaseFieldDataType(field);
            }
            if (ExportMiscellaneousUtils.getCoreRecordTypes().contains(itemType)) {
                String primaryKey = "";
                if (itemType.equals(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_WORKFLOW)) {
                    primaryKey = "\"ID\"" + "," + "\"StateID\"";
                }
                createQuery = "CREATE TABLE " + keyspaceTemplate + "." + tableName + "("
                        + "\"ID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT + columnNames
                        + ", PRIMARY KEY(" + primaryKey + "));";
                cassandraDAO.dbOperations(createQuery);
            } else {
                createQuery = "CREATE TABLE " + keyspaceTemplate + "." + tableName + "("
                        + "\"ID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                        + ", \"LanguageID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                        + ", PRIMARY KEY(\"LanguageID\",\"ID\")" + columnNames + ");";
                cassandraDAO.dbOperations(createQuery);

                if (cassandraDAO.getDatabaseType().equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_DB_TYPE_CASSANDRA)) {
                    createSasiIndex(itemType, keyspaceTemplate);
                }
            }
        } catch (Exception exception) {
            logError("Exception while creating item table using headers.", exception);
            throw new ExportStagingException(exception);
        }
        logger.info(tableName + " Table created successfully");
    }

    @Override
    public boolean deleteRow(String itemType, String message) {
        tableName = getTableName(itemType);
        String deleteQuery;
        String prepareLanguageIDs = "";
        if (ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType)) {
            prepareLanguageIDs = "\"LanguageID\" IN(" + getLanguageIds() + ") " + "AND ";
        }
        deleteQuery = "DELETE FROM " + keyspaceTemplate + "." + tableName
                + " WHERE " + prepareLanguageIDs + "\"ID\" IN (" + message + ")";
        try {
            cassandraDAO.dbOperations(deleteQuery);
        } catch (Exception e) {
            logError("Exception while deleting item row.", e);
            logger.info("[" + masterSubscriber + "] Data not deleted for Item with ID(s): " + message);
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteRow(String itemType, String itemID, String sMessage) {
        tableName = getTableName(itemType);
        String deletedFieldName = null;
        String deletedFieldID = null;
        String deleteQuery;
        String exportCoreItemTypes = "";
        List<String> deletedTypes = new ArrayList<>();
        try {
            //TODO refactor for identify the affected item ids from messsage or not ?
            deleteQuery = "DELETE FROM "
                    + keyspaceTemplate + "."
                    + tableName + " WHERE \"ID\" IN (" + itemID + ")";
            if (sMessage != null && !sMessage.isEmpty()) {
                List<String> messageList = Arrays.asList(sMessage.replaceAll(" ", "").split(","));
                if (messageList.size() > 1) {
                    deletedTypes.add(messageList.get(1).toLowerCase());
                    deletedFieldName = messageList.get(2);
                    deletedFieldID = messageList.get(3);
                }
                if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_WORKFLOW)) {
                    exportCoreItemTypes = String.join(",", ExportMiscellaneousUtils.getCoreItemTypes());
                } else if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE)) {
                    deletedTypes = ExportMiscellaneousUtils.getCoreItemTypes();
                }

                for (String affectedType : deletedTypes) {
                    affectedType = affectedType.replace("\"", "").toLowerCase();
                    if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_WORKFLOW)) {
                        if ("StateID".equals(deletedFieldName)) {
                            deleteQuery += " AND \"StateID\" = " + deletedFieldID;
                        }
                        if (exportCoreItemTypes.toLowerCase().contains(affectedType.toLowerCase()))
                        updateAffectedItems(affectedType, deletedFieldName, deletedFieldID);
                    } else if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE)) {
                        deleteAffectedItems(affectedType, deletedFieldName, deletedFieldID);
                    }
                }
            }
            cassandraDAO.dbOperations(deleteQuery);
        } catch (Exception e) {
            logError("Data not deleted for Item with ID: " + itemID, e);
            return false;
        }
        return true;
    }

    @Override
    public String getHeaderKey(String dataType) {
        return dataType.toLowerCase();
    }

    private void updateAffectedItems(String deletedType, String deletedFieldName, String deletedFieldID) {
        String affectedItemIds = "";
        long itemID;
        try {
            String selectQuery = "SELECT \"ID\", \"LanguageID\""
                    + " FROM " + keyspaceTemplate + "." + getMaterializedViewName(getTableName(deletedType), deletedFieldName)
                    + " WHERE \"" + deletedFieldName + "\" = " + deletedFieldID
                    + " AND \"LanguageID\" IN (" + getLanguageIds() + ")";

            ResultSet resultSet = cassandraDAO.dbOperations(selectQuery);
            List<Row> rows = resultSet.all();
            for (Row row : rows) {
                itemID = row.getLong("ID");
                String updateQuery = "UPDATE " + keyspaceTemplate + "." + getTableName(deletedType) +
                        " SET \"WorkflowID\" = 0, \"StateID\" = 0" +
                        " WHERE \"ID\" = " + itemID +
                        " AND \"LanguageID\" = " + row.getLong("LanguageID");
                cassandraDAO.dbOperations(updateQuery);
                affectedItemIds += itemID + ",";
            }
            if (rows.size() > 0) {
                String logMessage = "[" + masterSubscriber + "] Affected items data updated for " + deletedType
                        + " on deletion of " + deletedFieldName + ": " + deletedFieldID + " and affected item";
                logger.info(logMessage + " count: " + rows.size());
                logger.debug(logMessage + "(s):" + affectedItemIds);
            } else {
                logger.info("[" + masterSubscriber + "] : No affected items founds for " + deletedType + " to update on deletion of " + deletedFieldName + ":" + deletedFieldID);
            }
        } catch (Exception e) {
            logError("Data not updated for affected Item ID(s): " + affectedItemIds, e);
        }
    }

    private void deleteAffectedItems(String deletedType, String deletedFieldName, String deletedFieldID) {
        String deleteAffectedItemsQuery = "DELETE FROM " + keyspaceTemplate + "." + getTableName(deletedType) +
                " WHERE \"" + deletedFieldName + "\"=" + deletedFieldID;
        try {
            ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, deletedType);
            cassandraDAO.dbOperations(deleteAffectedItemsQuery);
            logger.info("[" + masterSubscriber + "] Affected items deleted successfully because of " + deletedFieldName + " deletion with ID" + deletedFieldID);
        } catch (ExportStagingException e) {
            logError("Exception while deleting " + deletedType + " affected items for deleted " + deletedFieldName + "ID: " + deletedFieldID, e);
        } finally {
            ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
        }
    }

    private void createSasiIndex(String itemType, String keyspace) throws ExportStagingException {
        String indexColumns[] = {"ClassMapping", "_Parents"};
        String sIndexQuery;
        try {
            ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
            for (String column : indexColumns) {
                sIndexQuery = "CREATE CUSTOM INDEX IF NOT EXISTS INDEX_" + itemType.toUpperCase() + "_" + column + " ON " + keyspace + "." +
                        tableName + "(\"" + column + "\") using 'org.apache.cassandra.index.sasi.SASIIndex' " +
                        "WITH OPTIONS = { 'mode': 'CONTAINS' };";
                cassandraDAO.dbOperations(sIndexQuery);
            }
        } catch (Exception e) {
            logError("Exception while creating SASI index.", e);
        } finally {
            ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
        }
    }
}