package com.exportstaging.moderators;

import com.exportstaging.abstractclasses.AbstractDataProvider;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Component;

import java.util.*;


@Component("itemUpdater")
class ItemUpdater extends ModeratorData
{

  private static final String COLUMN_NAMES_KEY  = "columnNames";
  private static final String COLUMN_VALUES_KEY = "columnValues";


  /**
   * Insertion/updation/deletion of Item in all languages will be perform here
   *
   * @param itemID    String id of an object
   * @param items     JSONArray of object in all languages
   * @param itemType  type of an object
   * @param action    int action for which object is came(create or delete)
   * @param deleteRow boolean flag for row deletion
   *
   * @return status of object, if successfully operation is done then true otherwise false
   */
  boolean updateItemTable(String itemID, JSONArray items, String itemType, int action, boolean deleteRow)
  {
    boolean status;
    int     isCreated = 0;
    try {
      if (deleteRow) {
        dataProviderItem.deleteRow(itemType, itemID);
      }
      List list;
      if (action == mActionCreate) {
        isCreated = 1;
      }
      Map<String, Map<String, Object>> fieldValueMapping = new HashMap<>();
      Set                              columnHeaders     = new HashSet();

      for (Object item : items) {
        String              languageID   = "0";
        Map<String, Object> fieldMapping = new HashMap();
        JSONObject          oCurrentItem = (JSONObject) item;
        columnHeaders = oCurrentItem.keySet();
        list = new ArrayList();
        for (Object columnHeader : columnHeaders) {
          String columnHeaderName = (String) columnHeader;
          Object objectValue      = oCurrentItem.get(columnHeaderName);

          if (objectValue instanceof JSONArray) {
            ((JSONArray) objectValue).forEach(list::add);
            if (!list.isEmpty()) {
              for (Object jsonString : list) {
                JSONArray jsonObject = new JSONArray();
                jsonObject.add(jsonString);
                this.updateItemTable(itemID, jsonObject, itemType, action, false);
              }
            }
          }
          else {
            fieldMapping.put("\"" + columnHeader + "\"", objectValue);
          }
        }
        for (String field : AbstractDataProvider.esaDataInDate) {
          String cassandraColumnName = "\"" + field + "\"";
          if (field.equals(ExportMiscellaneousUtils.EXPORT_DATABASE_FIELD_CREATION_DATE) && isCreated == 0) {
            continue;
          }

          fieldMapping.put(cassandraColumnName, "toTimestamp(now())");
        }
        for (String field : AbstractDataProvider.esaDataInInt) {
          String cassandraColumnName = "\"" + field + "\"";
          fieldMapping.put(cassandraColumnName, Integer.toString(isCreated));
        }
        if (ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType)) {
          languageID = (String) oCurrentItem.get(ExportMiscellaneousUtils.EXPORT_FIELD_LANGUAGEID);
        }

        fieldValueMapping.put(languageID, fieldMapping);
      }

      String tableName = dataProviderItem.getTableName(itemType);
      status = dataProviderItem.insertItemData(itemType, tableName, fieldValueMapping);
    } catch (Exception exception) {
      logError("Exception while updating item data for ID: " + itemID, exception, masterSubscriber);
      return false;
    }
    return status;
  }


  /**
   * Object reference will be processed here
   * All insertion/deletion/updation of an object references will be done here
   *
   * @param itemID   String Id of an object
   * @param message  ItemMessage Item message which contains reference data
   * @param itemType String type of an object
   *
   * @return status of object, if successfully operation is done then true otherwise false
   */
  boolean updateReferenceTable(String itemID, JSONArray message, String itemType)
  {
    boolean status;
    try {
      dataProviderReference.deleteRow(itemType, itemID);
      Map<String, Map<String, Map<String, Object>>> fieldValueMapping = new HashMap<>();

      if (message != null) {
        for (Object item : message) {
          Map<String, Map<String, Object>> refAttributeValueMapping = new HashMap<>();
          Map<String, Object>              fieldData                = new HashMap<>();
          JSONObject                       oCurrentItem             = (JSONObject) item;
          Set                              columnHeaders            = oCurrentItem.keySet();
          String                           csReferenceId            = null;
          String                           refLanguageId            = null;
          for (Object ColumnHeader : columnHeaders) {
            String columnHeaderName = (String) ColumnHeader;
            Object objectValue      = oCurrentItem.get(columnHeaderName);
            fieldData.put("\"" + columnHeaderName + "\"", objectValue);
            if (columnHeaderName.equalsIgnoreCase("CSReferenceID")) {
              csReferenceId = (String) objectValue;
            }
            if (columnHeaderName.equalsIgnoreCase("LanguageID")) {
              refLanguageId = (String) objectValue;
            }
          }
          refAttributeValueMapping.put(csReferenceId, fieldData);
          if (fieldValueMapping.putIfAbsent(refLanguageId, refAttributeValueMapping) != null) {
            fieldValueMapping.get(refLanguageId).put(csReferenceId, fieldData);
          }
        }

        status = dataProviderReference.insertReferenceData(itemType, dataProviderReference.getTableName(itemType),
                                                           fieldValueMapping);
      }
      else {
        return true;
      }
    } catch (Exception exception) {
      logError("Exception while updating reference data for ID: " + itemID, exception, masterSubscriber);
      return false;
    }
    return status;
  }


  /**
   * Returns a new object of Map which can be used to hold data for insert query.
   *
   * @param columnName  String of comma separated column names
   * @param columnValue String of comma separated column values
   *
   * @return a new {@link HashMap} with two keys. These keys can be accessed using ItemUpdater.COLUMN_NAMES_KEY
   * and ItemUpdater.COLUMN_VALUES_KEY respectively
   */
  private Map<String, String> getSingleRowMap(String columnName, String columnValue)
  {
    Map<String, String> singleRowMap = new HashMap<>();
    singleRowMap.put(COLUMN_NAMES_KEY, columnName);
    singleRowMap.put(COLUMN_VALUES_KEY, columnValue);
    return singleRowMap;
  }


  /**
   * Object Subtable will be processed here
   * All insertion/deletion/updation of an object subtable will be done here
   *
   * @param itemID   String Id of an object
   * @param message  ItemMessage Item message which contains subtable data
   * @param itemType String type of an object
   *
   * @return status of object, if successfully operation is done then true otherwise false
   */
  boolean updateSubtableTable(String itemID, JSONArray message, String itemType)
  {
    boolean status;
    try {
      dataProviderSubtable.deleteRow(itemType, itemID);
      Map<String, Map<String, Map<String, Object>>> fieldValueMapping = new HashMap<>();
      if (message != null) {
        for (Object item : message) {
          Map<String, Map<String, Object>> tableAttributeValueMapping = new HashMap<>();
          Map<String, Object>              fieldData                  = new HashMap<>();
          String                           languageId                 = null;
          String                           rowId                      = null;
          JSONObject                       oCurrentItem               = (JSONObject) item;
          Set                              columnHeaders              = oCurrentItem.keySet();

          for (Object ColumnHeader : columnHeaders) {
            String columnHeaderName = (String) ColumnHeader;
            Object objectValue      = oCurrentItem.get(columnHeaderName);

            if (columnHeaderName.equalsIgnoreCase("LanguageID")) {
              languageId = (String) objectValue;
            }
            if (columnHeaderName.equalsIgnoreCase("ItemTableID")) {
              rowId = (String) objectValue;
            }
            fieldData.put("\"" + columnHeaderName + "\"", objectValue);
          }
          tableAttributeValueMapping.put(rowId, fieldData);
          if (fieldValueMapping.putIfAbsent(languageId, tableAttributeValueMapping) != null) {
            fieldValueMapping.get(languageId).put(rowId, fieldData);
          }
        }


        String tableName = dataProviderSubtable.getTableName(itemType);
        status = dataProviderSubtable.insertSubtableData(itemType, tableName, fieldValueMapping);
      }
      else {
        return true;
      }
    } catch (Exception exception) {
      logError("Exception while updating sub table data for ID: " + itemID, exception, masterSubscriber);
      return false;
    }
    return status;
  }


  boolean deleteItem(String itemID, String itemType, List<String> ids)
  {
    boolean deleteItemData, deleteReferenceData, deleteSubtableData;
    String  message = String.join(",", ids);
    if (ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType)) {
      deleteItemData = dataProviderItem.deleteRow(itemType, message);
      deleteReferenceData = dataProviderReference.deleteRow(itemType, message);
      deleteSubtableData = dataProviderSubtable.deleteRow(itemType, message);
      return deleteItemData && deleteReferenceData && deleteSubtableData;
    }
    else {
      return dataProviderItem.deleteRow(itemType, itemID, message);
    }
  }
}
