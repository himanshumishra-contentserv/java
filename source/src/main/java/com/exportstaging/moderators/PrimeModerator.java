package com.exportstaging.moderators;

import com.exportstaging.domain.ConfigurationMessage;
import com.exportstaging.domain.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("primeModerator")
public class PrimeModerator extends ModeratorData {
    @Autowired
    private ItemModerator itemModerator;
    @Autowired
    private ConfigurationModerator configurationModerator;
    @Autowired
    private MappingModerator mappingModerator;


    public boolean setItem(Message message, String itemType, String type) {
        return mTypeItem.equals(type) && itemModerator.setItems(message, itemType);
    }

    public boolean setConfigurations(ConfigurationMessage message, String itemType, String type) {
        if (type.equals(mTypeConfiguration)) {
            return configurationModerator.setConfiguration(message, itemType);
        } else if (type.equals(mTypeMapping)) {
            return mappingModerator.setMappings(message,itemType);
        }
        return false;
    }

    public boolean deleteItem(List<String> ids, String itemType, String type) {
        boolean status = false;
        if (type.equals(mTypeItem)) {
            status = itemModerator.deleteItem(ids, itemType);
        } else if (type.equals(mTypeConfiguration)) {
            status = configurationModerator.deleteConfiguration(itemType, ids);
        }
        return status;
    }
}