package com.exportstaging.moderators;

import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.domain.ConfigurationMessage;
import org.apache.log4j.MDC;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component("configurationModerator")
public class ConfigurationModerator extends ModeratorData {
    private Map<String, String> columnNameWithDataType = null;

    boolean setConfiguration(ConfigurationMessage message, String itemType) {
        boolean status = false;
        boolean isItemTableUpdated = false, isReferenceTableUpdated = false, isSubtableTableUpdated = false;
        String itemID = "";
        try {
            itemID = message.getId();
            columnNameWithDataType = dataProviderConfiguration.getColumnNameWithDataType(dataProviderConfiguration.getTableName(itemType));
            JSONObject messageObject = message.getParsedConfiguration();
            if (message.getConfigurationType().equals(mKeyItem)) {
                isItemTableUpdated = updateItemTable(itemID, messageObject, itemType);
            } else if (message.getConfigurationType().equals(mKeyReference)) {
                isReferenceTableUpdated = updateReferenceTable(messageObject, itemType);
            } else if (message.getConfigurationType().equals(mKeySubtable)) {
                isSubtableTableUpdated = updateSubtableTable(messageObject, itemType);
            } else {
                logger.warn("Invalid json key for getting configuration data for ID: " + itemID);
            }
            status = isItemTableUpdated || isReferenceTableUpdated || isSubtableTableUpdated;
        } catch (Exception exception) {
            logError("Exception while processing configuration data for ID: " + itemID, exception, masterSubscriber);
        }
        return status;
    }

    boolean deleteConfiguration(String itemType, List<String> ids) {
        int isLink = dataProviderConfiguration.getIsLink(itemType, ids.get(0));
        boolean bCheckFlag;
        switch (isLink) {
            case 0: {
                bCheckFlag = dataProviderConfiguration.deleteRow(itemType, String.join(",", ids));
                //TODO check single single query for drop column
                for (String columnID : ids) {
                    dataProviderItem.dropAttributeColumns(itemType, columnID);
                    //TODO No need to call for all Item type
                    dataProviderItem.dropAttributeColumns(itemType + sSuffixView, columnID);
                    dataProviderReference.dropAttributeColumns(itemType, columnID);
                    dataProviderSubtable.dropAttributeColumns(itemType, columnID);
                }
                break;
            }
            case 1: {
                bCheckFlag = dataProviderConfiguration.deleteRow(itemType, String.join(",", ids));
                break;
            }
            default: {
                bCheckFlag = true;
                break;
            }
        }
        return bCheckFlag;
    }

    private boolean updateConfigurationTable(JSONObject jsonObject, String itemType, String TypeID) {
        Set<String> extraColumns = new HashSet<>(columnNameWithDataType.keySet());
        String columnName = "";
        String columnValue = "";
        for (Object key : jsonObject.keySet()) {
            String fieldKey = key.toString();
            if (jsonObject.get(fieldKey) != null) {
                String fieldValue = jsonObject.get(fieldKey).toString();
                String fieldDataType = columnNameWithDataType.get(fieldKey);
                extraColumns.remove(fieldKey);
                columnName += "\"" + fieldKey + "\",";
                columnValue += dataProviderConfiguration.getColumnValue(fieldDataType, fieldValue);
            }
        }
        columnName = columnName.concat("\"" + ExportMiscellaneousUtils.getExportDatabaseFieldTypeId() + "\",");
        columnValue = columnValue.concat(TypeID + ",");
        extraColumns.remove(ExportMiscellaneousUtils.getExportDatabaseFieldTypeId());
        String[] insertQueryData = dataProviderConfiguration.prepareExtraColumns(extraColumns, columnName, columnValue);
        return dataProviderConfiguration.insertColumnData(itemType, insertQueryData[0], insertQueryData[1]);
    }

    private boolean updateItemTable(String itemID, JSONObject message, String itemType) {
        String typeID = (message.get("IsFolder").equals("1")) ? "0" : "1";
        if (typeID.equals("1")) {
            dataProviderItem.addAttributeColumns(itemType, itemID.trim());
            if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_PDMARTICLE)
                    && ExportMiscellaneousUtils.getConfiguredTypes().contains(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_PDMARTICLESTRUCTURE)) {
                //MDC is a ugly hack here to ensure the "ItemType" part of the logs go proper.
                //Without this we will see logs something like this:
                //2018-03-28 19:05:56	admin	Pdmarticle	 INFO	Column 483:FormattedValue successfully added to export_pdmarticlestructure Table	ExportDatabase
                //Note: Observe the ItemType and the updated table name.
                MDC.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_PDMARTICLESTRUCTURE);
                dataProviderItem.addAttributeColumns(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_PDMARTICLESTRUCTURE, itemID.trim());
                MDC.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
            }
        }
        return updateConfigurationTable(message, itemType, typeID);
    }

    private boolean updateReferenceTable(JSONObject message, String itemType) {
        boolean status = updateConfigurationTable(message, itemType, "2");
        String subAttributes = (String) message.get(ExportMiscellaneousUtils.getExportDatabaseFieldSubAttributes());
        if (subAttributes != null && !subAttributes.isEmpty()) {
            String ids[] = subAttributes.trim().split(",");
            for (String columnId : ids) {
                dataProviderReference.addAttributeColumns(itemType, columnId.trim());
            }
        }
        return status;
    }

    private boolean updateSubtableTable(JSONObject message, String itemType) {
        boolean status = updateConfigurationTable(message, itemType, "3");
        String subAttributes = (String) message.get(ExportMiscellaneousUtils.getExportDatabaseFieldSubAttributes());
        if (subAttributes != null && !subAttributes.isEmpty()) {
            String ids[] = subAttributes.trim().split(",");
            for (String columnId : ids) {
                dataProviderSubtable.addAttributeColumns(itemType, columnId.trim());
            }
        }
        return status;
    }
}
