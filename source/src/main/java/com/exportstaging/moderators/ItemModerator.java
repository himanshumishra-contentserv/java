package com.exportstaging.moderators;

import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.domain.ItemMessage;
import com.exportstaging.domain.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("itemModerator")
public class ItemModerator extends ModeratorData {
    @Autowired
    private ItemUpdater itemUpdater;


    public boolean setItems(Message message, String itemType) {
        boolean status = false;
        boolean statusItem, statusReference, statusSubtable;
        String id = "";
        try {
            id = message.getId();
            int action = message.getAction();
            boolean deleteDataEntry = true;
            if (ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType)) {
                deleteDataEntry = false;
            }
            statusItem = itemUpdater.updateItemTable(id, message.getParsedItem(), itemType, action, deleteDataEntry);
            //Following AND condition added to ensure Item is added before adding subtable/reference data
            if (ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType) && statusItem) {
                ItemMessage itemMessage = (ItemMessage) message;
                statusReference = itemUpdater.updateReferenceTable(id, itemMessage.getParsedReference(), itemType);
                statusSubtable = itemUpdater.updateSubtableTable(id, itemMessage.getParsedSubtable(), itemType);
                if (statusReference && statusSubtable) {
                    status = true;
                }
            } else {
                if (statusItem) {
                    status = true;
                }
            }
        } catch (Exception e) {
            logError("Exception while processing item data for ID: " + id, e, masterSubscriber);
        }
        return status;
    }

    public boolean deleteItem(List<String> ids, String itemType) {
        try {
            //First element is always the ID of the message for delete scenario
            return itemUpdater.deleteItem(ids.get(0), itemType, ids);
        } catch (Exception e) {
            logError("Exception while deleting item data for ID: " + ids, e, masterSubscriber);
        }
        return false;
    }
}
