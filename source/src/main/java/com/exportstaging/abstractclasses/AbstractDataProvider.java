package com.exportstaging.abstractclasses;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.connectors.database.CassandraDAO;
import com.exportstaging.connectors.database.DatabaseConnection;
import com.exportstaging.dao.DataProvider;
import com.exportstaging.utils.CassandraListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;

import static com.exportstaging.common.ExportMiscellaneousUtils.TAB_DELIMITER;


public abstract class AbstractDataProvider implements DataProvider {
    private static final String UNKNOWN_IDENTIFIER = "Unknown identifier ";
    private static final String UNDEFINED_COLUMN = "Undefined column name ";
    /**
     * Following field would be use while creating the table during the initial export - Cassandra
     */
    public static String esaDataInInt[] = {"_IsCreated"};
    public static String esaDataInDate[] = {"_LastWritten", "_InsertTime"};
    protected static List<String> itemViewFieldNames = new ArrayList<>();
    protected static List<String> itemStructureViewFieldNames = new ArrayList<>();
    protected static List<String> numericDataTypesList = new ArrayList<>();
    protected static List<String> nonNumericDataTypesList = new ArrayList<>();
    protected static List<String> collectionDataTypesList = new ArrayList<>();
    protected static List<String> exportDateTypeField = new ArrayList<>();
    protected static List<String> exportIntegerTypeField = new ArrayList<>();

    static {
        String pdmarticleViewFields[] = {"ParentID",
                "IsFolder",
                "StateID",
                "_IsCreated",
                "_LastWritten",
                "_InsertTime",
                "LanguageID",
                "WorkflowID"};
        String pdmarticleStructureViewFields[] = {"ParentID",
                "IsFolder",
                "StateID",
                "_IsCreated",
                "_LastWritten",
                "_InsertTime",
                "_ExtensionID",
                "LanguageID",
                "WorkflowID"};
        String numericDataTypes[] = {ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT,
                ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT,
                ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_FLOAT,
                ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_DOUBLE};
        String nonNumericDataTypes[] = {ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT,
                ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TIMESTAMP};

        String collectionDataTypes[] = {ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_LIST_INT,};
        itemViewFieldNames.addAll(Arrays.asList(pdmarticleViewFields));
        itemStructureViewFieldNames.addAll(Arrays.asList(pdmarticleStructureViewFields));
        numericDataTypesList.addAll(Arrays.asList(numericDataTypes));
        nonNumericDataTypesList.addAll(Arrays.asList(nonNumericDataTypes));
        collectionDataTypesList.addAll(Arrays.asList(collectionDataTypes));
        exportDateTypeField.addAll(Arrays.asList(esaDataInDate));
        exportIntegerTypeField.addAll(Arrays.asList(esaDataInInt));
    }

    protected final String CONST_EXPORT_ITEM_STRUCTURE = "structure";
    protected final String CONST_EXPORT_LANGUAGE_ID = "LanguageID";
    protected final Logger logger = LogManager.getLogger("exportstaging");
    @Autowired
    protected DatabaseConnection databaseConnection;
    @Autowired
    protected CassandraListener listener;
    @Autowired
    public CassandraDAO cassandraDAO;
    @Value("${cassandra.prefix.export}")
    protected String prefixExport;
    @Value("${export.tools.connection.delay}")
    protected int retryDelay;
    @Value("${activemq.subscriber.master}")
    protected String masterSubscriber;
    @Value("${core.project.name}")
    protected String projectName;
    protected String keyspaceTemplate = "keyspace_template";
    private String tableName;
    private Map<String, Set<String>> cacheColumnFamily = new HashMap<>();


    @Override
    public boolean checkTable(String itemType) throws ExportStagingException {
        tableName = getTableName(itemType);
        return cassandraDAO.checkTable(tableName);
    }


    @Override
    public boolean checkColumn(String itemType, String columnName) {
        tableName = getTableName(itemType);
        try {
            return cassandraDAO.checkColumn(tableName, columnName);
        } catch (Exception e) {
            logError("Exception while checking if column exists.", e);
        }
        return false;
    }


    @Override
    public List<String> getColumns(String itemType) {
        List<String> columnList = new ArrayList<>();
        tableName = getTableName(itemType);
        try {
            columnList = cassandraDAO.getColumns(tableName);
        } catch (Exception e) {
            logError("Exception while fetching columns of table " + tableName, e);
        }
        return columnList;
    }


    @Override
    public boolean addColumn(String itemType, String columnName) {
        tableName = getTableName(itemType);
        try {
            if (checkTable(itemType)) {
                if (!checkColumn(itemType, columnName)) {
                    String sAlterQuery = "ALTER TABLE " + keyspaceTemplate + "." + tableName + " ADD " + "\"" + columnName +
                            "\"" + getExportDatabaseFieldDataType(
                            columnName) + ";";
                    cassandraDAO.dbOperations(sAlterQuery);
                    cassandraDAO.dynamicAddColumnNameWithDataType(tableName, columnName,
                            getExportDatabaseFieldDataType(columnName));
                    logger.info("Column " + columnName + " successfully added to " + tableName + " Table");
                }
            }
        } catch (Exception e) {
            logError("Failed to add column " + columnName + " in table " + tableName, e);
            return false;
        }
        return true;
    }


    @Override
    public void addAttributeColumns(String itemType, String attributeID) {
        tableName = getTableName(itemType);
        String sColumnF = attributeID + ":" + "FormattedValue";
        String sColumnV = attributeID + ":" + "Value";
        addColumn(itemType, sColumnF);
        addColumn(itemType, sColumnV);
    }


    @Override
    public boolean insertColumnData(String itemType, String columnNames, String columnValues) {
        tableName = getTableName(itemType);
        columnNames = columnNames.trim();
        try {
            String insertQuery = getQuery(columnNames, columnValues);
            cassandraDAO.dbOperations(insertQuery);
        } catch (ExportStagingException e) {
            if (e.getMessage().contains(UNKNOWN_IDENTIFIER)) {
                String columnName = e.getMessage().substring(UNKNOWN_IDENTIFIER.length(), e.getMessage().length());
                if (addColumn(itemType, columnName)) {
                    return insertColumnData(itemType, columnNames, columnValues);
                } else {
                    logError("Failed to add " + columnName + " column in " + tableName + " Table", e);
                    return false;
                }
            } else if (e.getMessage().contains(UNDEFINED_COLUMN)) {
                String columnName = e.getMessage()
                        .substring(UNDEFINED_COLUMN.length(), e.getMessage().length())
                        .replace("\"", "");
                if (addColumn(itemType, columnName)) {
                    return insertColumnData(itemType, columnNames, columnValues);
                } else {
                    logError("Failed to add " + columnName + " column in " + tableName + " Table", e);
                    return false;
                }
            } else {
                logError("Exception while inserting column data." + tableName, e);
                return false;
            }
        }
        return true;
    }


    private String getQuery(String columnNames, String columnValues) {
        return "INSERT INTO " + keyspaceTemplate + "." + tableName + " (" + columnNames + ") VALUES(" + columnValues + ");";
    }


    public List getColumnFamily(String itemType) throws ExportStagingException {
        return cassandraDAO.getColumns(getTableName(itemType));
    }


    /**
     * Insertion of object will be happen here
     * Dynamic column creation will be done here
     *
     * @param itemType          String type of an object
     * @param tableName         String name of a table for which insertion will be done
     * @param fieldValueMapping Map of field and its values
     *                          Map structure would be <LanguageID, <FieldName, FieldVale>>
     * @return true if object insertion done successfully otherwise false
     */
    public boolean insertItemData(String itemType, String tableName, Map<String, Map<String, Object>> fieldValueMapping) {
        Set<BoundStatement> bindStatement = new HashSet<>();
        Boolean batchStatus = true;
        try {
            Map<String, String> columnNameWithDataType = getColumnFamilyWithType(tableName);
            for (Map.Entry<String, Map<String, Object>> fieldEntry : fieldValueMapping.entrySet()) {
                Map<String, Object> attributeValueMapping = fieldEntry.getValue();
                updateRemainingAttribute(attributeValueMapping, columnNameWithDataType.keySet());
                cassandraDAO.processItemData(tableName, attributeValueMapping, columnNameWithDataType, bindStatement);
            }
            batchStatus = cassandraDAO.executeBatch(bindStatement);
        } catch (Exception exception) {
            String message = exception.getMessage();
            if (message.contains(UNKNOWN_IDENTIFIER)) {
                String columnName = message.substring(UNKNOWN_IDENTIFIER.length(), message.length());
                if (addColumn(itemType, columnName)) {
                    insertItemData(itemType, tableName, fieldValueMapping);
                } else {
                    logError("Failed to add " + columnName + " column in " + tableName + " Table", exception);
                    return false;
                }
            } else if (message.contains(UNDEFINED_COLUMN)) {
                String[] split = message.split(" ");
                String missingColumn = split[split.length - 1];
                missingColumn = missingColumn.replace("\"", "").replace(")", "");
                Boolean status = addColumn(itemType, missingColumn);
                insertItemData(itemType, tableName, fieldValueMapping);
                if (status) {
                    insertItemData(itemType, tableName, fieldValueMapping);
                } else {
                    logError("Failed to add " + missingColumn + " column in " + tableName + " Table", exception);
                    return false;
                }
            } else {
                logError("Exception while inserting data in bulk. Table name: " + tableName, exception);
                return false;
            }
        }

        return batchStatus;
    }


    /**
     * Insertion of Reference data for object will be done here
     *
     * @param tableName         String Name of table
     * @param fieldValueMapping Map of field and ots Value
     *                          Structure of Map would be <LanguageID,<ReferenceAttributeID,<ClassOfReferenceField,
     *                          Value>>>
     * @return true if insertion of reference data is done successfully otherwise false
     * @throws ExportStagingException object if any exception occurs
     */
    public boolean insertReferenceData(
            String itemType, String tableName, Map<String, Map<String, Map<String, Object>>> fieldValueMapping
    ) throws ExportStagingException {
        return processReferenceOrSubtableData(itemType, tableName, fieldValueMapping);
    }


    /**
     * Insertion of Subtable data for object will be done here
     *
     * @param tableName         String Name of table
     * @param fieldValueMapping Map of field and ots Value
     *                          Structure of Map would be <LanguageID,<SubtableAttributeID,<ClassOfSubtableField,
     *                          Value>>>
     * @return true if insertion of Subtable data is done successfully otherwise false
     * @throws ExportStagingException object if any exception occurs
     */
    public boolean insertSubtableData(
            String itemType, String tableName, Map<String, Map<String, Map<String, Object>>> fieldValueMapping
    ) throws ExportStagingException {
        return processReferenceOrSubtableData(itemType, tableName, fieldValueMapping);
    }


    /**
     * Insertion of reference/Subtable data for object will be done here
     *
     * @param tableName         String Name of table
     * @param fieldValueMapping Map of field and ots Value
     *                          Structure of Map would be <LanguageID,<SubtableAttributeID,<ClassOfSubtableField,
     *                          Value>>>
     * @return true if insertion of reference/Subtable data is done successfully otherwise false
     * @throws ExportStagingException object if any exception occurs
     */
    private boolean processReferenceOrSubtableData(
            String itemType, String tableName, Map<String, Map<String, Map<String, Object>>> fieldValueMapping
    ) {
        boolean batchStatus = true;
        try {
            Map<String, String> columnNameWithDataType = getColumnFamilyWithType(tableName);
            Set<BoundStatement> bindStatement = new HashSet<>();
            for (String languageID : fieldValueMapping.keySet()) {
                for (String csRefId : fieldValueMapping.get(languageID).keySet()) {
                    Map<String, Object> attributeValueDetails = fieldValueMapping.get(languageID).get(csRefId);
                    updateRemainingAttribute(attributeValueDetails, columnNameWithDataType.keySet());
                    cassandraDAO.processReferenceOrSubtableData(tableName, attributeValueDetails, columnNameWithDataType,
                            bindStatement);
                }
            }
            batchStatus = cassandraDAO.executeBatch(bindStatement);
        } catch (Exception exception) {
            String message = exception.getMessage();
            if (message.contains(UNKNOWN_IDENTIFIER)) {
                String columnName = message.substring(UNKNOWN_IDENTIFIER.length());
                if (addColumn(itemType, columnName)) {
                    processReferenceOrSubtableData(itemType, tableName, fieldValueMapping);
                } else {
                    logError("Failed to add " + columnName + " column in " + tableName + " Table", exception);
                    return false;
                }
            } else if (message.contains(UNDEFINED_COLUMN)) {
                String[] split = message.split(" ");
                String missingColumn = split[split.length - 1];
                missingColumn = missingColumn.replace("\"", "").replace(")", "");
                Boolean status = addColumn(itemType, missingColumn);
                processReferenceOrSubtableData(itemType, tableName, fieldValueMapping);
                if (status) {
                    processReferenceOrSubtableData(itemType, tableName, fieldValueMapping);
                } else {
                    logError("Failed to add " + missingColumn + " column in " + tableName + " Table", exception);
                    return false;
                }
            } else {
                logError("Exception while inserting data in bulk. Table name: " + tableName, exception);
                return false;
            }
        }
        return batchStatus;
    }


    /**
     * Method will provide all column from cassandra table with their data type for provided object type
     *
     * @param tableName String type of object
     * @return Map of column name with their data type
     */
    public Map<String, String> getColumnFamilyWithType(String tableName) {
        Map<String, String> columnNameWithDataType = getColumnNameWithDataType(tableName);
        Map<String, String> columnNameWithType = new HashMap<>();
        for (Map.Entry<String, String> columnDetails : columnNameWithDataType.entrySet()) {
            columnNameWithType.put("\"" + columnDetails.getKey() + "\"", columnDetails.getValue());
        }
        return columnNameWithType;
    }


  /**
   * Method will update the field with "" which are exists in Cassandra table but not present tn fieldValueMapping
   * for current object
   *
   * @param fieldValueMapping Map of field and its values
   * @param cacheColumnFamily Set of columnName from coumnfamily
   *
   * @return Updated field value map
   */
  private Map<String, Object> updateRemainingAttribute(
    Map<String, Object> fieldValueMapping, Set<String> cacheColumnFamily
  )
  {
    Set<String> providedColumn = fieldValueMapping.keySet();
    for (String cassandraColumn : cacheColumnFamily) {
      if (!providedColumn.contains(
        cassandraColumn) && !(("\"" + ExportMiscellaneousUtils.EXPORT_DATABASE_FIELD_CREATION_DATE + "\"")
        .equalsIgnoreCase(cassandraColumn))) {
        fieldValueMapping.put(cassandraColumn, "");
      }
    }

    return fieldValueMapping;
  }


    public boolean bulkInsertData(String itemType, List<Map<String, String>> insertData) {
        tableName = getTableName(itemType);
        List<String> queries = new ArrayList<>();
        for (Map<String, String> singleRow : insertData) {
            String columnNames = singleRow.get("columnNames");
            String columnValues = singleRow.get("columnValues");
            queries.add(getQuery(columnNames, columnValues));
        }
        try {

            cassandraDAO.bulkInsert(queries);
            //cassandraDAO.bindFiledData(itemType, tableName, insertData);
        } catch (ExportStagingException e) {
            if (e.getMessage().contains(UNKNOWN_IDENTIFIER)) {
                String columnName = e.getMessage().substring(UNKNOWN_IDENTIFIER.length(), e.getMessage().length());
                if (addColumn(itemType, columnName)) {
                    return bulkInsertData(itemType, insertData);
                } else {
                    logError("Failed to add " + columnName + " column in " + tableName + " Table", e);
                    return false;
                }
            } else if (e.getMessage().contains(UNDEFINED_COLUMN)) {
                String columnName = e.getMessage()
                        .substring(UNDEFINED_COLUMN.length(), e.getMessage().length())
                        .replace("\"", "");
                if (addColumn(itemType, columnName)) {
                    return bulkInsertData(itemType, insertData);
                } else {
                    logError("Failed to add " + columnName + " column in " + tableName + " Table", e);
                    return false;
                }
            } else {
                logError("Exception while inserting data in bulk. Table name: " + tableName, e);
                return false;
            }
        }
        return true;
    }


    @Override
    public boolean dropColumn(String itemType, String columnName) {
        tableName = getTableName(itemType);
        boolean status = false;
        try {
            if (checkColumn(itemType, columnName)) {
                String alterQuery = "ALTER TABLE " + keyspaceTemplate + "." + tableName + " DROP \"" + columnName + "\"";
                cassandraDAO.dbOperations(alterQuery);
                status = true;
                logger.info("Column " + columnName + " successfully dropped from " + tableName + " Table");
            }
        } catch (Exception e) {
            String eMessage = e.getMessage();
            if (e instanceof ExportStagingException && (eMessage.contains("Cannot drop column " + columnName))) {
                logger.info(
                        "[" + masterSubscriber + "] Cannot drop column " + columnName + " for item type " + itemType + " due to " + "limitation on cassandra.");
                status = true;
            } else {
                logError("Failed to Drop Column for ItemType: " + itemType + " and ColumnName:(" + columnName + ").", e);
            }
        } finally {
            cassandraDAO.removeColumnNameWithDataType(tableName, columnName);
        }
        return status;
    }


    protected String prepareINOperatorDataQuery(String itemIds) {
        itemIds = itemIds.replace(", ", "\', \'");
        itemIds = itemIds.replace("[", "\'");
        itemIds = itemIds.replace("]", "\'");
        return itemIds;
    }


    protected String getLanguageIds() {
        Set<Integer> languageIds = new HashSet<>();
        try {
            String languageQuery =
                    "SELECT \"" + ExportMiscellaneousUtils.EXPORT_FIELD_ID + "\" FROM " + keyspaceTemplate + ".export_language;";
            ResultSet resultSet = cassandraDAO.dbOperations(languageQuery);
            for (Row row : resultSet) {
                languageIds.add(row.getInt(ExportMiscellaneousUtils.EXPORT_FIELD_ID));
            }
        } catch (Exception e) {
            logError("Exception while fetching languageIDs: ", e);
        }

        return StringUtils.join(languageIds, ',');
    }


    @Override
    public boolean dropAttributeColumns(String itemType, String attributeID) {
        tableName = getTableName(itemType);
        boolean bStatusF;
        boolean bStatusV;
        try {
            String sColumnF = attributeID + ":" + "FormattedValue";
            String sColumnV = attributeID + ":" + "Value";
            bStatusF = dropColumn(itemType, sColumnF);
            bStatusV = dropColumn(itemType, sColumnV);
        } catch (Exception e) {
            logError("Failed to drop column from table:" + tableName, e);
            return false;
        }
        return bStatusF && bStatusV;
    }


    @Override
    public void dropTable(String itemType) throws ExportStagingException {
        tableName = getTableName(itemType);
        if (checkTable(itemType)) {
            String dropQuery;
            try {
                dropQuery = "DROP TABLE " + keyspaceTemplate + "." + tableName + ";";
                cassandraDAO.dbOperations(dropQuery);
                logger.info("[" + masterSubscriber + "] " + tableName + " Table successfully dropped");
            } catch (Exception e) {
                logError("Failed to drop table " + tableName, e);
            }
        }
    }


    @Override
    public boolean truncateTable(String itemType) {
        String truncateQuery;
        tableName = getTableName(itemType);
        try {
            truncateQuery = "TRUNCATE " + keyspaceTemplate + "." + tableName;
            cassandraDAO.dbOperations(truncateQuery);
            return true;
        } catch (Exception e) {
            logError("Failed to Truncate Table " + tableName, e);
        }
        logger.info(tableName + " Table truncated successfully");
        return false;
    }


    @Override
    public Map<String, String> getColumnNameWithDataType(String tableName) {
        try {
            return cassandraDAO.getColumnNameWithDataType(tableName);
        } catch (Exception e) {
            logError("Exception while fetching column data type.", e);
        }
        return new HashMap<>();
    }


    @Override
    public String getColumnValue(String dataType, Object objectValue) {
        if (dataType == null || dataType.equals("")) {
            //TODO Need to reload the column family for newly added columns
            dataType = ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT;
        }

        if (objectValue == null) {
            if (nonNumericDataTypesList.contains(dataType)) {
                return "'',";
            } else if (numericDataTypesList.contains(dataType)) {
                return objectValue + ",";
            } else if (collectionDataTypesList.contains(dataType)) {
                return "[],";
            }
        } else {
            objectValue = objectValue.toString().replace("'", "''");
            if (nonNumericDataTypesList.contains(dataType)) {
                return "'" + objectValue + "',";
            } else if (numericDataTypesList.contains(dataType)) {
                return objectValue + ",";
            } else if (collectionDataTypesList.contains(dataType)) {
                String[] IDs = objectValue.toString().trim().split(" ");
                if (IDs.length == 0) {
                    return "[],";
                } else {
                    return Arrays.toString(IDs) + ",";
                }
            }
        }
        return "";
    }


    public String[] prepareExtraColumns(Set<String> extraColumns, String columnName, String columnValue) {
        String[] insertQueryData = new String[2];
        for (String extraColumn : extraColumns) {
            columnName += "\"" + extraColumn + "\",";
            columnValue += null + ",";
        }
        insertQueryData[0] = columnName.substring(0, columnName.length() - 1).trim();
        insertQueryData[1] = columnValue.substring(0, columnValue.length() - 1);
        return insertQueryData;
    }


    public ResultSet getItemResultSet(String itemType, String itemId, String languageId, String columnNames) {
        tableName = getTableName(itemType);
        String selectQuery =
                "SELECT " + columnNames + " FROM  " + keyspaceTemplate + "." + tableName + " WHERE \"" + ExportMiscellaneousUtils.EXPORT_FIELD_LANGUAGEID + "\" = " + languageId + " AND \"" + ExportMiscellaneousUtils.EXPORT_FIELD_ID + "\" = " + itemId + ";";
        try {
            return cassandraDAO.dbOperations(selectQuery);
        } catch (ExportStagingException e) {
            logError("Exception in getting item result set data for item type:" + itemType, e);
        }
        return null;
    }


    public ResultSet getItemResultSet(String itemType, String itemId, List<String> languageIDs, String columnNames) {
        tableName = getTableName(itemType);
        String selectQuery =
                "SELECT " + columnNames + " FROM  " + keyspaceTemplate + "." + tableName + " WHERE \"" + ExportMiscellaneousUtils.EXPORT_FIELD_LANGUAGEID + "\" IN ( " + String
                        .join(",", languageIDs) + ") AND \"" + ExportMiscellaneousUtils.EXPORT_FIELD_ID + "\" = " + itemId + ";";

        try {
            return cassandraDAO.dbOperations(selectQuery);
        } catch (ExportStagingException e) {
            logError("Exception while getting item resultset data for item type:" + itemType, e);
        }
        return null;
    }


    protected String getExportDatabaseFieldDataType(String columnName) {
        if (exportDateTypeField.contains(columnName)) {
            return ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TIMESTAMP;
        } else if (exportIntegerTypeField.contains(columnName)) {
            return ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT;
        } else {
            return ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT;
        }
    }


    protected void logError(String message, Exception e) {
        logger.error(
                "[" + masterSubscriber + "]:" + message + " Error Message:" + e.getMessage());
        logger.debug("[" + masterSubscriber + "]:" + message + " Error Message:" + e.getMessage() + TAB_DELIMITER + Arrays.toString(
                e.getStackTrace()));
    }
}
