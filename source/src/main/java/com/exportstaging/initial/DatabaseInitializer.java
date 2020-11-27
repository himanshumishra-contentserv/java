package com.exportstaging.initial;

import com.datastax.driver.core.ResultSet;
import com.exportstaging.abstractclasses.AbstractDataProvider;
import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.connectors.database.CassandraDAO;
import com.exportstaging.connectors.database.DatabaseConnection;
import com.exportstaging.dataprovider.*;
import com.exportstaging.exportdb.CassandraStrategyOptionEnum;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

import static com.exportstaging.common.ExportMiscellaneousUtils.*;

@Component
public class DatabaseInitializer {

    @Value("${core.project.name}")
    private String projectName;
    @Value("${cassandra.suffix.view}")
    private String sSuffixView;
    @Value("${cassandra.replicationfactor}")
    private String replicationFactor;
    @Value("${json.key.reference}")
    private String typeReference;
    @Value("${cassandra.keyspace}")
    private String keyspace;
    @Value("${cassandra.replication.strategy}")
    private String replicationStrategy;
    @Value("${cassandra.replication.network.topology}")
    private String replicationNetworkTopology;


    @Autowired
    private DatabaseConnection databaseConnection;
    @Autowired
    private DataProviderItem dataProviderItem;
    @Autowired
    private DataProviderReference dataProviderReference;
    @Autowired
    private DataProviderSubtable dataProviderSubtable;
    @Autowired
    private DataProviderMapping dataProviderMapping;
    @Autowired
    private DataProviderConfiguration dataProviderConfiguration;
    @Autowired
    private DataProviderLanguage dataProviderLanguage;
    @Autowired
    private CassandraDAO cassandraDAO;

    private String keyspaceTemplate = "keyspace_template";

    protected final Logger logger = LogManager.getLogger("exportstaging");


    public boolean checkKeyspace() throws CannotCreateConnectionException {
        return isKeyspaceExisting();
    }

    public boolean checkLanguageTable() throws ExportStagingException {
        return dataProviderLanguage.checkTable(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE);
    }

    public boolean checkReferenceTable() throws ExportStagingException {
        return dataProviderReference.checkTable(typeReference);
    }

    public boolean checkItemTable(String itemType) throws ExportStagingException {
        return dataProviderItem.checkTable(itemType);
    }

    public boolean checkItemSubtableTable(String itemType) throws ExportStagingException {
        return dataProviderSubtable.checkTable(itemType);
    }

    public boolean checkItemConfigurationTable(String itemType) throws ExportStagingException {
        return dataProviderConfiguration.checkTable(itemType);
    }

    public boolean checkItemMappingTable(String itemType) throws ExportStagingException {
        return dataProviderMapping.checkTable(itemType);
    }

    public void createLanguageTable() throws ExportStagingException {
        tableCreator(dataProviderLanguage, ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE, null);
    }

    public void createItemTable(String itemType, JSONObject dataModel, List<String> configuredMVFields) throws ExportStagingException {
        JSONObject combinedJson = new JSONObject();
        if (getCoreRecordTypes().contains(itemType)) {
            combinedJson.putAll((JSONObject) dataModel.get(ExportMiscellaneousUtils.ES_FIELDS_STANDARD));
            tableCreator(dataProviderItem, itemType, combinedJson);
        }
        else if (ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType)) {
            combinedJson.putAll((JSONObject)dataModel.get(ExportMiscellaneousUtils.ES_FIELDS_STANDARD));
            ((JSONObject)dataModel.get(ExportMiscellaneousUtils.ES_FIELDS_CUSTOM)).keySet().forEach(key -> {
                combinedJson.put(key, ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT);
            });
            tableCreator(dataProviderItem, itemType, combinedJson);
            createMaterializedView(itemType, configuredMVFields);
        }
    }

    public void createConfigurationTable(String itemType, JSONObject dataModel) throws ExportStagingException {
        tableCreator(dataProviderConfiguration, itemType, dataModel);
    }

    public void createItemSubtable(String itemType, JSONObject dataModel) throws ExportStagingException {
        tableCreator(dataProviderSubtable, itemType, dataModel);
    }

    public void createMappingTable(String itemType) throws ExportStagingException {
        tableCreator(dataProviderMapping, itemType, null);
    }

    public void createReferenceTable(JSONObject dataModel) throws ExportStagingException {
        tableCreator(dataProviderReference, typeReference, dataModel);
    }

    private void tableCreator(AbstractDataProvider dataProvider, String itemType, JSONObject dataModel) throws ExportStagingException {
        dataProvider.createTableFromHeaders(itemType, dataModel);
        String tableName = dataProvider.getTableName(itemType);
        System.out.println(tableName + " table created!");
        logger.info(tableName + " table created!");
    }

    private void createMaterializedView(String itemType, List<String> configuredMVFields) throws ExportStagingException
    {
        try {
            dataProviderItem.createMaterializedView(itemType, configuredMVFields);
            System.out.println(itemType + " Materialized Views Created!");
            logger.info(itemType + " Materialized Views Created!");
        } catch (ExportStagingException exception) {
            logger.error("Unable to create materialized view. error :" + exception.getMessage());
        }
    }

    private boolean isKeyspaceExisting() throws CannotCreateConnectionException {
        try {
            return cassandraDAO.isKeyspaceExisting();
        } catch (ExportStagingException e) {
            if (e.isConnectionException()) {
                logError("Exception while connecting to "+ getDatabaseType(), e, getDatabaseType());
                throw new CannotCreateConnectionException("Failed to connect to " + getDatabaseType());
            }
            logger.warn(e.getMessage());
            return false;
        } catch (Exception e) {
            logger.warn(e.getMessage());
            return false;
        }
    }


    /**
     * Provide the database type, Cassandra or ScyllaDB
     *
     * @return database type, Cassandra or ScyllaDB
     */
    public String getDatabaseType()
    {
        String databaseType = null;
        try {
            databaseType = cassandraDAO.getDatabaseType();
        } catch (ExportStagingException e) {
            logError("Exception while identifying database type", e, "MaserSubscriber");
        }

        return databaseType;
    }


    /**
     * Defines and creates the cassandra keyspace
     */
    public void keyspaceCreator() {

        String createQuery = "";

        if (CassandraStrategyOptionEnum.SIMPLE_STRATEGY.getStrategy().equalsIgnoreCase(replicationStrategy)) {

            createQuery = "CREATE KEYSPACE " + keyspaceTemplate +
                    " WITH REPLICATION = { 'class' : '" + replicationStrategy + "'," +
                    " 'replication_factor': '" + replicationFactor + "' }" +
                    " AND DURABLE_WRITES = true;";

        } else if (CassandraStrategyOptionEnum.NETWORK_TOPOLOGY_STRATEGY.getStrategy().equalsIgnoreCase(replicationStrategy)) {

            String[] replicationData = replicationNetworkTopology.split(",");
            createQuery = "CREATE KEYSPACE " + keyspaceTemplate + " WITH REPLICATION = { 'class' : '" + replicationStrategy + "'";

            for (String s: replicationData) {
                if (s.matches(".*:[0-9]+$")) {
                    String[] datacenter = s.split(":");
                    createQuery += String.format(",'%s' : '%s'", datacenter[0], datacenter[1]);
                }
            }
            createQuery += "} AND DURABLE_WRITES = true;";
        }

        try {
            cassandraDAO.dbOperations(createQuery);
            System.out.println("Keyspace " + keyspace + " Created");
            logger.info("Keyspace " + keyspace + " Created");
        } catch (Exception e) {
            logError("Exception while creating keyspace: " + keyspace, e, getDatabaseType());
        }
    }

    public boolean dropKeyspace() {
        try {
            if (isKeyspaceExisting()) {
                String dropQuery = "DROP keyspace " + keyspaceTemplate;
                ResultSet resultSet = cassandraDAO.dbOperations(dropQuery);
                if (resultSet != null && resultSet.wasApplied()) {
                    String infoMessage = "[" + getDatabaseType() + "] Keyspace " + keyspace + " dropped successfully";
                    logger.info(infoMessage);
                    System.out.println(infoMessage);
                } else {
                    logger.warn("[" + getDatabaseType() + "] Failed to drop keyspace " + keyspace);
                }
            }
        } catch (ExportStagingException | CannotCreateConnectionException e) {
            logError("Exception while dropping Keyspace: " + keyspace, e, "MaserSubscriber");
            return false;
        }
        return true;
    }

    private void dropMaterializedViews(String itemType) {
        try {
            cassandraDAO.dropMaterializedViews(itemType);
        } catch (ExportStagingException e) {
        }
    }

    public boolean removeItemTypes(List<String> itemTypes) {
        for (String itemType : itemTypes) {
            try {
                ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
                if (itemType.equals(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE)) {
                    continue;
                }
                dropMaterializedViews(itemType);
                dataProviderItem.dropTable(itemType);
                //TODO refactor to make it generic and remove hard-coded values
                if (itemType.contains("Pdmarticle")) {
                    if ((!ExportMiscellaneousUtils.getCoreItemTypes().contains("Pdmarticle") && !ExportMiscellaneousUtils.getCoreItemTypes().contains("Pdmarticlestructure"))) {
                        dataProviderConfiguration.dropTable(itemType);
                        dataProviderMapping.dropTable(itemType);
                        dataProviderSubtable.dropTable(itemType);
                    }
                }
                if (itemType.equals("Mamfile") || itemType.equals("User")) {
                    dataProviderConfiguration.dropTable(itemType);
                    dataProviderMapping.dropTable(itemType);
                    dataProviderSubtable.dropTable(itemType);
                }
                if (ExportMiscellaneousUtils.getCoreItemTypes().isEmpty()) {
                    dataProviderReference.dropTable(itemType);
                    dataProviderLanguage.dropTable(itemType);
                }
            } catch (ExportStagingException e) {
                logError("ExportStaging Exception while dropping table", e, "MaserSubscriber");
            } finally {
                ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
            }
        }
        return true;
    }

    public void closeConnection() {
        cassandraDAO.closeConnection();
    }

    private void logError(String message, Exception e, String component) {
        logger.error("[" + component + "]:" + message + " Error Message:" + e.getMessage());
        logger.debug("[" + component + "]:" + message + " Error Message:" + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
    }
}
