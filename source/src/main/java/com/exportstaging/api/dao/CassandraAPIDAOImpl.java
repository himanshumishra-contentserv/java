package com.exportstaging.api.dao;

import com.datastax.driver.core.Session;
import com.exportstaging.api.CassandraAPI;
import com.exportstaging.connectors.database.DatabaseConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CassandraAPIDAOImpl {
    @Autowired
    private DatabaseConnection databaseConnection;
    @Autowired
    private ItemAPIDAOImpl itemAPIDAOImpl;

    @Value("${cassandra.suffix.reference}")
    private String sSuffixReference;
    @Value("${cassandra.suffix.subtable}")
    private String sSuffixSubtable;

    private static final String EXPORT_CASSANDRA_INDEX = "INDEX";
    private static final String EXPORT_CASSANDRA_INDEX_DELIMETER = "_";
    private static final String EXPORT_CASSANDRA_INDEX_OPERATOR = " ON ";
    private static final String EXPORT_CASSANDRA_EXCEPTION = "Export cassandra Error :  ";
    private static final String EXPORT_CASSANDRA_INDEX_QUERY_CREATE = "CREATE INDEX IF NOT EXISTS ";
    private static final String EXPORT_CASSANDRA_INDEX_QUERY_DROP = "DROP INDEX IF EXISTS ";

    public boolean removeIndex(String itemType, String subItemType, String fieldName) {
        String tableName = "";
        if (subItemType == null) {
            tableName = itemAPIDAOImpl.getTableName(itemType);
        } else if (subItemType.equalsIgnoreCase(CassandraAPI.ITEM_TYPE_SUBTABLE)) {
            tableName = itemAPIDAOImpl.getTableName(itemType, sSuffixSubtable);
        } else if (subItemType.equalsIgnoreCase(CassandraAPI.ITEM_TYPE_REFERENCE)) {
            tableName = itemAPIDAOImpl.getTableName(itemType, sSuffixReference);
        } else {
            return false;
        }
        return dropIndex(tableName, fieldName);
    }

    public boolean createIndex(String itemType, String subItemType, String fieldName) {
        String tableName = "";
        if (subItemType == null) {
            tableName = itemAPIDAOImpl.getTableName(itemType);
        } else if (subItemType.equalsIgnoreCase(CassandraAPI.ITEM_TYPE_SUBTABLE)) {
            tableName = itemAPIDAOImpl.getTableName(itemType, sSuffixSubtable);
        } else if (subItemType.equalsIgnoreCase(CassandraAPI.ITEM_TYPE_REFERENCE)) {
            tableName = itemAPIDAOImpl.getTableName(itemType, sSuffixReference);
        } else {
            return false;
        }
        return addIndex(tableName, fieldName);
    }

    private boolean addIndex(String tableName, String fieldName) {
        String indexQuery = null;
        String indexName = null;
        try {
            indexName = EXPORT_CASSANDRA_INDEX + EXPORT_CASSANDRA_INDEX_DELIMETER + tableName
                    + EXPORT_CASSANDRA_INDEX_DELIMETER + fieldName;
            if (fieldName.contains(":")) {
                indexName = EXPORT_CASSANDRA_INDEX + EXPORT_CASSANDRA_INDEX_DELIMETER
                        + tableName + EXPORT_CASSANDRA_INDEX_DELIMETER + fieldName.replace(":", EXPORT_CASSANDRA_INDEX_DELIMETER);
            }
            indexQuery = EXPORT_CASSANDRA_INDEX_QUERY_CREATE + indexName + EXPORT_CASSANDRA_INDEX_OPERATOR
                    + databaseConnection.getKeyspace() + "." + tableName + "(\"" + fieldName + "\");";
            databaseConnection.getSession().execute(indexQuery);
        } catch (Exception e) {
            System.out.println(EXPORT_CASSANDRA_EXCEPTION + e.getMessage());
            return false;
        }
        return true;
    }

    private boolean dropIndex(String tableName, String fieldName) {
        String indexName = EXPORT_CASSANDRA_INDEX + EXPORT_CASSANDRA_INDEX_DELIMETER + tableName
                + EXPORT_CASSANDRA_INDEX_DELIMETER + fieldName;
        if (fieldName.contains(":")) {
            indexName = EXPORT_CASSANDRA_INDEX + EXPORT_CASSANDRA_INDEX_DELIMETER
                    + tableName + EXPORT_CASSANDRA_INDEX_DELIMETER + fieldName.replace(":", EXPORT_CASSANDRA_INDEX_DELIMETER);
        }
        try {
            String indexQuery = EXPORT_CASSANDRA_INDEX_QUERY_DROP + databaseConnection.getKeyspace() + ". " + indexName;
            databaseConnection.getSession().execute(indexQuery);
        } catch (Exception e) {
            System.out.println(EXPORT_CASSANDRA_EXCEPTION + e.getMessage());
            return false;
        }
        return true;
    }

    public void close() {
        if (databaseConnection != null) {
            databaseConnection.closeConnection();
        }
    }
}
