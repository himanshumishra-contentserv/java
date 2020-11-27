package com.exportstaging.moderators;

import com.exportstaging.domain.ConfigurationMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("mappingModerator")
public class MappingModerator extends ModeratorData {
    @Value("${cassandra.table.mapping.classid}")
    private String columnClassID;
    @Value("${cassandra.table.mapping.attributeid}")
    private String columnAttributeID;

    public boolean setMappings(ConfigurationMessage exportMessage, String itemType) {
        boolean sStatus;
        String id = "";
        try {
            id = exportMessage.getId();
            List<String> sLinkedIDs = exportMessage.getMappingData();
            if (id.equals("-1")) {
                sStatus = truncateCassandra(itemType);
            } else {
                sStatus = updateMappingTable(id.trim(), sLinkedIDs, itemType);
            }
            if (sStatus) {
                return true;
            }
        } catch (Exception exception) {
            logError("Exception while setting mapping data for ID: " + id, exception, masterSubscriber);
        }
        return false;
    }

    private boolean truncateCassandra(String sItemType) {
        return dataProviderMapping.truncateTable(sItemType);
    }

    private boolean updateMappingTable(String classID, List<String> linkedIDs, String itemType) {
        try {
            String columnNames;
            String columnValues;
            for (String sLinkedID : linkedIDs) {
                columnNames = "\"" + columnClassID + "\", \"" + columnAttributeID + "\"";
                columnValues = classID.trim() + ", " + sLinkedID.trim();
                dataProviderMapping.insertColumnData(itemType, columnNames, columnValues);
            }
        } catch (Exception exception) {
            logError("Exception while updating mapping data for ID: " + classID, exception, masterSubscriber);
            return false;
        }
        return true;
    }

}
