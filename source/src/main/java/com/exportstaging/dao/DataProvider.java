package com.exportstaging.dao;

import com.exportstaging.api.exception.ExportStagingException;
import org.json.simple.JSONObject;

import java.util.List;
import java.util.Map;

public interface DataProvider {

    void createTableFromHeaders(String itemType, JSONObject dataModel) throws ExportStagingException;

    String getTableName(String itemType);

    boolean checkTable(String itemType) throws ExportStagingException;

    boolean checkColumn(String itemType, String columnName);

    boolean addColumn(String itemType, String columnName);

    void addAttributeColumns(String itemType, String attributeID);

    boolean dropColumn(String itemType, String columnName);

    boolean dropAttributeColumns(String itemType, String attributeID);

    boolean deleteRow(String itemType, String itemID);

    // Method to delete a row for Record type
    boolean deleteRow(String itemType, String itemID, String sAffectedItemType);

    void dropTable(String itemType) throws ExportStagingException;

    boolean truncateTable(String itemType);

    boolean insertColumnData(String itemType, String columnNames, String columnValues);

    List<String> getColumns(String itemType);

    Map<String, String> getColumnNameWithDataType(String tableName);

    String getColumnValue(String dataType, Object objectValue);

    String getHeaderKey(String dataType);
}
