package com.exportstaging.connectors.idbconnector;

import com.exportstaging.api.exception.ExceptionLogger;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.initial.ExportInitializerUtils;
import com.exportstaging.producers.Producer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.stereotype.Component;

import java.net.SocketException;
import java.sql.SQLException;
import java.util.*;

@Component("idbConnector")
public class IntermediateDAO {

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ExportInitializerUtils exportInitializerUtils;

    @Value("${activemq.producer.master}")
    private String masterProducer;
    @Value("${activemq.producer.project}")
    private String projectProducer;
    @Value("${mysql.csdbprefix.name}")
    private String csDbPrefix;
    @Value("${mysql.prefix.exportstaging}")
    private String idbTablePrefix;
    @Value("${mysql.type.mapping}")
    private String typeMapping;
    @Value("${mysql.type.item}")
    private String typeItem;
    @Value("${mysql.type.configuration}")
    private String typeConfiguration;
    @Value("${mysql.type.filecontent}")
    private String typeFileContent;
    @Value("${mysql.column.subscribername}")
    private String columnSubscriberName;
    @Value("${mysql.column.producerid}")
    private String columnProducerId;
    @Value("${mysql.column.itemtype}")
    private String columnItemType;
    @Value("${data.json.file.suffix.headers}")
    private String headers;
    @Value("${mysql.column.message}")
    private String columnMessage;
    @Value("${mysql.column.data}")
    private String columnData;
    @Value("${mysql.column.type}")
    private String columnType;

    private final static Logger logger = LogManager.getLogger("exportstaging");
    private Map<String, JSONObject> headerData = new HashMap<>();

    /**
     * A single SQL update operation (such as an insert, update or delete statement).
     *
     * @param query static SQL to execute
     * @return true if succeed otherwise false
     * @throws ExportStagingException in case of exception ExportStagingException will be thrown.
     */
    public boolean executeQuery(String query) throws ExportStagingException {
        try {
            return (jdbcTemplate.update(query) > 0);
        } catch (DataAccessException e) {
            handleDataAccessException(e);
            return executeQuery(query);
        } catch (Exception e) {
            ExceptionLogger.logError("Exception while communicating with Intermediate Database.", e, masterProducer);
        }
        return false;
    }

    /**
     * Fetch the data for a query to a result list, given static SQL
     * <p>The results will be mapped to a List (one entry for each row) of
     * Maps (one entry for each column using the column name as the key)
     *
     * @param query SQL query to execute
     * @return an List that contains a Map per row
     * @throws ExportStagingException if there is any problem executing the query
     */
    public List<Map<String, Object>> fetchData(String query) throws ExportStagingException {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            result = jdbcTemplate.queryForList(query);
        } catch (DataAccessException e) {
            handleDataAccessException(e);
            return fetchData(query);
        } catch (Exception e) {
            ExceptionLogger.logError("Exception while communicating with Intermediate Database.", e, masterProducer);
        } catch (Throwable throwable) {
            ExceptionLogger.logError(throwable.getMessage(), throwable, masterProducer);
        }
        return result;
    }

    /**
     * Inserting the subscriber name and its supported item types mapping entries in mapping table
     *
     * @param subscriberName Name if subscriber
     * @param itemTypes      List of supported item types.
     */
    public void insertSubscriberItemTypeMapping(String subscriberName, List<String> itemTypes, boolean isCustomSubscriber) {
        try {
            int producerID = Producer.masterProducerID;
            if (isCustomSubscriber) {
                producerID = Producer.projectProducerID;
            }
            executeBatchQueriesUsingPrepareStatement(subscriberName, itemTypes, producerID);
        } catch (Exception e) {
            ExceptionLogger.logError("Exception while insertSubscriberItemTypeMapping.", e, subscriberName);
        }
    }

    /**
     * Removes the entries from subscriber item type mapping table for given subscriber.
     *
     * @param subscriberName Subscriber name for which removes the entries.
     * @throws ExportStagingException Throws an ExportDatabase exception in case of failure.
     */
    public void removeEntriesFromSubscriberMappingTable(String subscriberName) throws ExportStagingException {
        if (executeQuery(prepareMappingDeleteQuery(subscriberName, null))) {
            logger.info("[" + subscriberName + "] : Non-durable subscriber mapping entries deleted");
        }
    }

    /**
     * Removes the entries from subscriber item type mapping table for given subscriber and item types list.
     *
     * @param subscriberName Subscriber name for which removes the entries.
     * @param itemTypes      List of item types.
     * @throws ExportStagingException Throws an ExportDatabase exception in case of failure.
     */
    public void removeEntriesFromSubscriberMappingTable(String subscriberName, List<String> itemTypes) throws ExportStagingException {
        if (executeQuery(prepareMappingDeleteQuery(subscriberName, itemTypes))) {
            logger.info("[" + subscriberName + "] : Non-durable subscriber mapping entries deleted");
        }
    }


    /**
     * Create the export intermediate database tables based on list of pass item types.
     *
     * @param itemType List of item types
     */
    public void createTableForItemType(List<String> itemType) {
        List<String> createQueries = new ArrayList<>();
        for (String item : itemType) {
            createQueries.add(getQueryForCreateTable(csDbPrefix + idbTablePrefix + "_" + item));
            createQueries.add(getQueryForCreateTable(csDbPrefix + idbTablePrefix + "_" + item + "_" + typeConfiguration));
            createQueries.add(getQueryForCreateTable(csDbPrefix + idbTablePrefix + "_" + item + "_" + typeMapping));

            if (item.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILE))
                createQueries.add(getQueryForCreateTable(csDbPrefix + idbTablePrefix + "_" + item + "_" + typeFileContent));
        }
        createQueries.add(getQueryForCreateTable(csDbPrefix + idbTablePrefix + "_" + ExportMiscellaneousUtils.EXPORT_TYPE_OPERATION));
        createQueries.add(getQueryForCreateSubscriberItemMappingTable(getSubscriberItemMappingTableName()));
        try {
            executeBatchQueries(createQueries);
        } catch (ExportStagingException e) {
            ExceptionLogger.logError("Exception while creating tables for ItemType: " + itemType, e, masterProducer);
        }
    }

    /**
     * Drop the export intermediate database tables based on pass item types.
     *
     * @param itemTypes List of item types.
     */
    public void dropIntermediateTableForItemType(List<String> itemTypes) {
        List<String> deleteQueries = new ArrayList<>();
        for (String item : itemTypes) {
            if (getProducerIdSumFromMappingTable(item) == -1) {
                deleteQueries.add(getDropTableQuery(csDbPrefix + idbTablePrefix + "_" + item));
                deleteQueries.add(getDropTableQuery(csDbPrefix + idbTablePrefix + "_" + item + "_" + typeConfiguration));
                deleteQueries.add(getDropTableQuery(csDbPrefix + idbTablePrefix + "_" + item + "_" + typeMapping));

                if (item.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILE))
                    deleteQueries.add(getDropTableQuery(csDbPrefix + idbTablePrefix + "_" + item + "_" + typeFileContent));
            }
        }
        try {
            if (deleteQueries.size() > 0)
                executeBatchQueries(deleteQueries);
        } catch (ExportStagingException e) {
            ExceptionLogger.logError("Exception while dropping IDB tables for ItemType: " + itemTypes, e, masterProducer);
        }
    }

    private String getQueryForCreateSubscriberItemMappingTable(String tableName) {
        String idbCreateQuery = "CREATE TABLE IF NOT EXISTS IDB_TABLE_NAME_XXX(" +
                columnSubscriberName + " VARCHAR(200) NOT NULL," +
                columnItemType + " VARCHAR(200) NOT NULL," +
                columnProducerId + " SMALLINT(6) NOT NULL DEFAULT 0," +
                " PRIMARY KEY (" + columnSubscriberName + "," + columnItemType + "))";
        return idbCreateQuery
                .replaceAll("IDB_TABLE_NAME_XXX", tableName.toLowerCase()); //Table Name replace
    }

    private String getQueryForCreateTable(String tableName) {
        String idbCreateQuery = "CREATE TABLE IF NOT EXISTS IDB_TABLE_NAME_XXX( ID BIGINT(20) NOT NULL, Message LONGTEXT CHARACTER SET utf8 COLLATE utf8_unicode_ci, Action SMALLINT(6) NOT NULL DEFAULT '1', InsertTime BIGINT(20) UNSIGNED NOT NULL, JMSPriority SMALLINT(6) NOT NULL DEFAULT '0', JobID BIGINT(20) NOT NULL DEFAULT '0', VersionNr MEDIUMINT(8) UNSIGNED NOT NULL DEFAULT '1', ProducerStatus smallint(6) NOT NULL DEFAULT '0',PRIMARY KEY (ID), KEY IDX_JMSPriority (JMSPriority) USING BTREE, KEY IDX_Action (Action) USING BTREE, KEY IDX_ProducerStatus (ProducerStatus) USING BTREE, KEY IDX_InsertTime (InsertTime) USING BTREE)";
        return idbCreateQuery
                .replaceAll("IDB_TABLE_NAME_XXX", tableName.toLowerCase()); //Table Name replace
    }

    private String getDropTableQuery(String tableName) {
        return "DROP TABLE IF EXISTS " + tableName.toLowerCase();
    }

    /**
     * Returns the subscriber supported item types from mapping table
     *
     * @param producerID Producer ID for which item types require like. ProducerID for core producer is 2 and custom producer is 4.
     * @return Return the list of supported item types.
     */
    public List<String> getItemTypesFromMappingTable(int producerID) {
        String condition = columnProducerId + " = " + producerID;
        return getItemTypesUsingCondition(condition);
    }

    /**
     * Returns the subscriber supported item types from mapping table
     *
     * @param subscriberName Name of the subscriber for which item types require.
     * @return Return the list of supported item types.
     */
    public List<String> getItemTypesFromMappingTable(String subscriberName) {
        String condition = columnSubscriberName + " = '" + subscriberName + "'";
        return getItemTypesUsingCondition(condition);
    }

    public int getProducerIdSumFromMappingTable(String itemType) {
        String selectQuery =
                "SELECT SUM(DISTINCT " + columnProducerId +
                        ") FROM " + getSubscriberItemMappingTableName() +
                        " WHERE " + columnItemType + " = '" + itemType + "'";
        Integer integer = jdbcTemplate.queryForObject(selectQuery, Integer.class);
        if (integer == null) {
            return -1;
        }
        return integer;
    }

    /**
     * Returns the removed item types from mapping table based on pass item types.
     *
     * @param subscriberName Name of the subscriber for which item types require.
     * @param itemTypes      List of item types.
     *
     * @return Returns the removed item types list
     */
    public List<String> getRemovedItemTypes(String subscriberName, List<String> itemTypes)
    {
        List<String> removedItemTypes = null;
        try {
            String selectQuery = "SELECT " + columnItemType +
                                 " FROM " + getSubscriberItemMappingTableName() +
                                 " WHERE " + columnSubscriberName + " = '" + subscriberName + "'" +
                                 " AND " + columnItemType + " NOT IN(" + prepareINQuery(itemTypes) + ")";

            removedItemTypes = jdbcTemplate.queryForList(selectQuery, String.class);
        } catch (Exception exception) {
            ExceptionLogger.logError("Exception while handling removed item type ", exception, masterProducer);
        }
        return removedItemTypes;
    }


    private String getMappingInsertQuery() {
        return "INSERT INTO " + getSubscriberItemMappingTableName() +
                " (" + columnSubscriberName + "," + columnItemType + "," + columnProducerId + ") VALUES(?,?,?)" +
                " ON DUPLICATE KEY UPDATE " + columnSubscriberName + " = ?, " + columnItemType + " = ?";
    }

    private List<String> getItemTypesUsingCondition(String filter)
    {
        List<String> itemTypeFromMapping = null;
        try {
            String selectQuery = "SELECT DISTINCT " + columnItemType
                                 + " FROM " + getSubscriberItemMappingTableName()
                                 + " WHERE " + filter;
            itemTypeFromMapping = jdbcTemplate.queryForList(selectQuery, String.class);
        } catch (Exception exception) {
            ExceptionLogger.logError("Exception while handling fetching item type ", exception, masterProducer);
        }
        return itemTypeFromMapping;
    }

    private boolean executeBatchQueries(List<String> queries) throws ExportStagingException {
        try {
            int[] queryStatus = jdbcTemplate.batchUpdate(queries.toArray(new String[0]));
            for (int status : queryStatus) {
                if (status == 0) {
                    return false;
                }
            }
            return true;
        } catch (DataAccessException e) {
            handleDataAccessException(e);
            return executeBatchQueries(queries);
        } catch (Exception e) {
            ExceptionLogger.logError("Exception while communicating with Intermediate Database.", e, masterProducer);
        }
        return false;
    }

    private boolean executeBatchQueriesUsingPrepareStatement(String subscriberName, List<String> itemTypes, int producerID) {
        String query = getMappingInsertQuery();
        try {
            return jdbcTemplate.execute(query, (PreparedStatementCallback<Boolean>) ps -> {
                for (String itemType : itemTypes) {
                    ps.setString(1, subscriberName);
                    ps.setString(2, itemType);
                    ps.setInt(3, producerID);
                    ps.setString(4, subscriberName);
                    ps.setString(5, itemType);
                    ps.addBatch();
                }
                int[] queryStatus = ps.executeBatch();
                for (int status : queryStatus) {
                    if (status == 0)
                        return false;
                }
                return true;
            });
        } catch (DataAccessException e) {
            try {
                handleDataAccessException(e);
            } catch (ExportStagingException e1) {
                ExceptionLogger.logError("Exception while insertSubscriberItemTypeMapping.", e1, subscriberName);
            }
            return executeBatchQueriesUsingPrepareStatement(subscriberName, itemTypes, producerID);
        }
    }

    private void handleDataAccessException(DataAccessException e) throws ExportStagingException {
        if (e instanceof CannotGetJdbcConnectionException) {
            //NOTE: The exception message needs action from User, so should print the exception message
            ExceptionLogger.logError("Cannot connect to Intermediate Database", e, masterProducer);
            throw new ExportStagingException("Cannot connect to Intermediate Database. Error " + e.getMessage());
        } else if (e.getRootCause() instanceof SQLException) {
            try {
                handleSQLException((SQLException) e.getRootCause());
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
        } else if (e.getRootCause() instanceof SocketException) {
            handleSocketException((SocketException) e.getRootCause());
        } else {
            ExceptionLogger.logError("DataAccessException Message", e, masterProducer);
        }

    }

    private void handleSQLException(SQLException e) throws InterruptedException {
        int sqlErrorCode = e.getErrorCode();
        int threadSleepTime;
        if (sqlErrorCode == 1146) {
            threadSleepTime = 60000;
        } else {
            threadSleepTime = 300000;
        }
        ExceptionLogger.logError(e.getMessage() + ". MySQL Error Code: " + sqlErrorCode, e, masterProducer);
        Thread.sleep(threadSleepTime);
    }

    private void handleSocketException(SocketException e) {
        ExceptionLogger.logError("Intermediate Database Connection Failure.", e, masterProducer);
        try {
            if (exportInitializerUtils.isMySQLRunning()) {
                String msg = "Intermediate Database is now online.";
                logInfo(msg, true);
            } else {
                Thread.sleep(10000);
            }
        } catch (InterruptedException interruption) {
            ExceptionLogger.logError("Interruption captured while communicating with Intermediate Database", interruption, masterProducer);
            Thread.currentThread().interrupt();
        }
    }

    private void logInfo(String message) {
        logger.info("[" + masterProducer + "]: " + message);
    }

    private void logInfo(String message, boolean printToConsole) {
        logInfo(message);
        if (printToConsole)
            logConsole(message);
    }

    private void logConsole(String message) {
        System.out.println("[" + masterProducer + "}: " + message);
    }

    private String addTableNamePrefix(String postFix) {
        return csDbPrefix + idbTablePrefix + "_" + postFix;
    }

    private String getSubscriberItemMappingTableName() {
        return addTableNamePrefix(ExportMiscellaneousUtils.EXPORT_IDB_TABLE_SUBSCRIBER_ITEM_MAPPING);
    }

    public Map<String, JSONObject> getHeaders(List<String> itemTypes) {
        return fetchHeaders(headerQuery(getWhereClauseForHeaders(itemTypes)));
    }

    public JSONObject getHeaders(String itemType, String type) {
        String headerKey = itemType;
        if (!type.equalsIgnoreCase("item")) {
            headerKey += "_" + type;
        }
        return fetchHeaders(headerQuery(getWhereClauseForHeaders(itemType, type))).get(headerKey.toLowerCase());
    }


    /**
     * Method will return both searchable and standard fields from IDB
     *
     * @param objectType String type of item
     * @return set of unique searchable and standard fileds
     */
    public Set<String> getSearchableAndStandardFields(String objectType) {
        Set<String> columnNames = new HashSet<String>();
        columnNames.addAll(this.getSearchableFields(objectType));
        columnNames.addAll(this.getStandardFields(objectType));

        return columnNames;
    }


    /**
     * Method will return standard fields from IDB
     *
     * @param objectType String type of item
     * @return set of unique searchable and standard fileds
     */
    public Set<String> getStandardFields(String objectType)
    {
        Map<String, String> searchableFields = (Map<String, String>) getItem(objectType).get(
          ExportMiscellaneousUtils.ES_FIELDS_STANDARD);

        Set<String> columnNames = searchableFields.keySet();

        return columnNames;
    }


    /**
     * Method will return searchable fields from IDB
     *
     * @param objectType String   type of item
     * @return set of unique searchable and standard fileds
     */
    public Set<String> getSearchableFields(String objectType)
    {
        Set<String> columnNames      = new HashSet<String>();
        Object      searchableFields = getItem(objectType).get(ExportMiscellaneousUtils.ES_FIELDS_SEARCHABLE);
        if (searchableFields instanceof JSONObject) {
            columnNames = ((JSONObject) searchableFields).keySet();
        }

        return columnNames;
    }


    /**
     * To get the searchable fields from IDB caching is implemented.
     * If attribute changes from non-searchable to searchable then we need to invalidate the cache.
     * If attribute changes from searchable to non-searchable then it will be available in search index and it will be
     * updated in search index but searching from UI can not be happened.
     *
     * @param objectType String type of an object
     */
    public void clearObjectCache(String objectType)
    {
        JSONObject item = getItem(objectType);
        if (item != null) {
            headerData.remove(objectType);
        }
    }
	

    private JSONObject getItem(String objectType)
    {
        JSONObject item = null;
        if (headerData.get(objectType) == null) {
            item = getHeaders(objectType, "item");
            headerData.put(objectType, item);
        }

        return headerData.get(objectType);
    }


    /**
     * Method will return searchable attributes which are of type reference
     *
     * @param objectType item type
     * @return set of searchable reference attribute ids
     */
    public Set<String> getSearchableReferences(String objectType) {
        Set<String> searchableReferences = new HashSet<>();
        JSONArray fields = (JSONArray) getItem(objectType).get(ExportMiscellaneousUtils.ES_FIELDS_SEARCHABLE_REFERENCES);
        if (fields != null) {
            searchableReferences.addAll(fields);
        }


        return searchableReferences;
    }


    /**
     * Method will return searchable attributes which are of type subtable
     *
     * @param objectType item type
     * @return set of searchable subtable attribute ids
     */
    public Set<String> getSearchableSubtables(String objectType) {
        Set<String> searchableSubtables = new HashSet<>();
        JSONArray fields = (JSONArray) getItem(objectType).get(ExportMiscellaneousUtils.ES_FIELDS_SEARCHABLE_SUBTABLES);
        if (fields != null) {
            searchableSubtables.addAll(fields);
        }

        return searchableSubtables;
    }


    private Map<String, JSONObject> fetchHeaders(String query) {
        Map<String, JSONObject> headerData = new HashMap<>();
        try {
            List<Map<String, Object>> results = fetchData(query);
            if (results.isEmpty()) {
                System.out.println("Header table is not populated. Possible causes:" +
                        " Export is in progress or Export is not started." +
                        " We will check again the header table in a second. " +
                        "if still get the same message please perform update data model to populate headers");

                logger.info("Header table is not populated. Possible causes: Export is in progress or Export is not started. We will check again the header table in a second.");
                logger.debug("Fetch sql query: " + query);
                Thread.sleep(6000);
                return fetchHeaders(query);
            }
            for (Map<String, Object> row : results) {
                String itemType = (String) row.get(columnItemType);
                String type = (String) row.get(columnType);
                JSONObject data = (JSONObject) JSONValue.parse((String) row.get(columnData));
                if (!type.equalsIgnoreCase("item")) {
                    itemType += "_" + type;
                }
                headerData.put(itemType.toLowerCase(), data);
            }
        } catch (ExportStagingException e) {
            ExceptionLogger.logError("Exception while fetching headers information.", e, masterProducer);
        } catch (InterruptedException e) {
            ExceptionLogger.logError("Interruption requested", e, masterProducer);
        }
        logger.debug("Header information from intermediate database: " + headerData);
        return headerData;
    }

    private String getWhereClauseForHeaders(List<String> itemTypes) {
        itemTypes = ExportMiscellaneousUtils.convertListElementToLowerCase(itemTypes);
        return "LOWER(" + columnItemType + ") IN (\'" + String.join("\',\'", itemTypes) + "\')";
    }

    private String getWhereClauseForHeaders(String itemType, String type) {
        return "LOWER(" + columnItemType + ") = \'" + itemType.toLowerCase() + "\' AND " +
                "LOWER(" + columnType + ") = \'" + type.toLowerCase() + "\'";
    }

    private String headerQuery(String whereClause) {
        String query = "SELECT "
                + columnItemType
                + "," + columnType
                + "," + columnData
                + " FROM " + addTableNamePrefix(headers);
        if (whereClause != null && !whereClause.isEmpty()) {
            query += " WHERE " + whereClause;
        }
        return query + ';';
    }

    private String prepareMappingDeleteQuery(String subscriberName, List<String> itemTypes) {
        String filter = "1=1";

        if (itemTypes != null) {
            filter = columnItemType + " IN(" + prepareINQuery(itemTypes) + ")";
        }
        return "DELETE FROM " + getSubscriberItemMappingTableName() +
                " WHERE " + columnSubscriberName + " = '" + subscriberName + "'" +
                " AND " + filter;
    }

    private StringBuilder prepareINQuery(List<String> itemTypes) {
        StringBuilder inQuery = new StringBuilder();
        for (String itemType : itemTypes) {
            inQuery.append("'").append(itemType).append("',");
        }
        if (inQuery.length() > 0) {
            inQuery.deleteCharAt(inQuery.length() - 1);
        }
        return inQuery;
    }

    /**
     * Returns list of language Ids from idb headers
     *
     * @return List of language Ids
     */
    public List<String> getLanguageIds()
    {
        JSONObject languageHeaders = getHeaders(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE, "item");
        String     languageIds     = (String) languageHeaders.get("LanguageIds");
        return Arrays.asList(languageIds.split(","));
    }
    
    /**
     * get fields from header table of given cs type
     * 
     * @param objectType cs item type
     * @param type cs type of a field
     * @param searchablility only searchable or all custom fields
     * @return set of fields of given cs type
     */
    public Set<String> getFields(String objectType, String type, String searchablility){
        Map<String, String> fields = (Map<String, String>) getItem(objectType).get(searchablility);
        
        Set<String> selectedFields = new HashSet<>();
        for(Map.Entry<String, String> entry : fields.entrySet()) {
            if(type.equals(entry.getValue())) {
                selectedFields.add(entry.getKey());
            }
        }
        
        return selectedFields;
    }
}
