package com.exportstaging.dataprovider;

import com.datastax.driver.core.ResultSet;
import com.exportstaging.abstractclasses.AbstractDataProvider;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component("dataProviderConfiguration")
public class DataProviderConfiguration extends AbstractDataProvider {
    @Value("${cassandra.suffix.configuration}")
    private String sSuffixConfiguration;
    @Value("${cassandra.suffix.view}")
    private String sSuffixView;
    @Value("${json.key.configuration}")
    private String sConfigurationHeader;

    private String tableName;

    @Override
    public String getTableName(String itemType) {
        String sItemType = itemType.toLowerCase();
        if (sItemType.contains(sSuffixView)) {
            sItemType = sItemType.substring(0, sItemType.length() - 9);
        }
        return prefixExport + "_" + sItemType + "_" + sSuffixConfiguration;
    }

    @Override
    public void createTableFromHeaders(String itemType, JSONObject dataModel) throws ExportStagingException {
        tableName = getTableName(itemType);
        String columnNames = "";
        try {
            Set headerData = dataModel.keySet();
            String columnType = "";

            for (Object key : headerData) {
                String field = key.toString();
                if (field.equals("ID") || field.equals(ExportMiscellaneousUtils.getExportDatabaseFieldModule()) ||
                        field.equals(ExportMiscellaneousUtils.getExportDatabaseFieldTypeId())) {
                    continue;
                }
                columnType = dataModel.get(field).toString();
                if (columnType.equals("")) {
                    columnType = ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT;
                }
                columnNames += ", \"" + field + "\" " + columnType;
            }
            String createQuery = "CREATE TABLE " + keyspaceTemplate + "." + tableName +
                    "(\"" + ExportMiscellaneousUtils.getExportDatabaseFieldModule() + "\" "
                    + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT
                    + ", \"ID\" " + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", \"" + ExportMiscellaneousUtils.getExportDatabaseFieldTypeId() + "\" "
                    + ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT
                    + ", PRIMARY KEY(\"ID\",\"" + ExportMiscellaneousUtils.getExportDatabaseFieldModule() + "\")" +
                    columnNames + ");";
            cassandraDAO.dbOperations(createQuery);
        } catch (Exception exception) {
            logError("Exception while creating tables using headers.",exception);
            throw new ExportStagingException(exception);
        }
        logger.info(tableName + " Table created successfully");
    }

    public int getIsLink(String itemType, String sConfigurationID) {
        tableName = getTableName(itemType);
        String sIsLinkQuery;
        int isLink = 0;
        sIsLinkQuery = "SELECT * FROM " + keyspaceTemplate + "." + tableName + " WHERE \"ID\" = " + sConfigurationID + ";";
        ResultSet resultSet = null;
        try {
            resultSet = cassandraDAO.dbOperations(sIsLinkQuery);
            isLink = resultSet.one().getInt("IsLink");
        } catch (NullPointerException e) {
            logger.info("Configuration data is not available for " + itemType + " with ID: " + sConfigurationID + ".");
        } catch (Exception e) {
            logError("Exception while checking configurationID: " + sConfigurationID + " is class or not.", e);
        }
        return isLink;
    }

    @Override
    public boolean deleteRow(String itemType, String message) {
        tableName = getTableName(itemType);
        String deleteQuery = "DELETE FROM " + keyspaceTemplate + "." + tableName + " WHERE \"ID\" IN (" + message + ")";
        try {
            cassandraDAO.dbOperations(deleteQuery);
        } catch (Exception e) {
            logError("Exception while deleting row.", e);
            logger.info("[" + masterSubscriber + "] Data not deleted for Configuration with ID(s): " + message);
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteRow(String itemType, String itemID, String sAffectedItemType) {
        //TODO overridden method for Record and there is no configuration
        return false;
    }

    @Override
    public String getHeaderKey(String dataType) {
        return (dataType + "_configuration").toLowerCase();
    }
}
