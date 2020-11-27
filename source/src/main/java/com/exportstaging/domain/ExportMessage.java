package com.exportstaging.domain;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import static com.exportstaging.common.ExportMiscellaneousUtils.*;

/**
 * <h1>Export Message</h1>
 * <h2>
 * The ExportMessage class is the standard export staging message format. <br>
 * This message is sent by MasterProducer to ActiveMQ. <br>
 * Subscribers receive object of ExportMessage on calling getExportMessage(). <br>
 * </h2>
 * <p>
 * These are the information provided by ExportMessage <br>
 * ID:          It specifies ItemID/ConfigurationID for which this message belongs. <br>
 * Action:      It specifies the action that needs to be performed. 1 for update, 2 for delete <br>
 * Message:     It is the Item/Configuration Data in JSON Format <br>
 * ItemType:    It specifies the Module Type <br>
 * Type:        It specifies Mapping/Configuration/Item <br>
 * ItemMessage: It specifies the complete json decoded message into item, reference, subtable
 * MessageType: It provides the information if the message if of type Item or Record
 * </p>
 *
 * @author Export Staging Team
 * @version 1.0
 * @since CS16.0
 */

public class ExportMessage {

    private static final String TYPE_MAPPING = "Mapping";
    private static final String TYPE_FILE_CONTENT = "FileContent";
    private static final String TYPE_CONFIGURATION = "Configuration";

    private int jobID;
    private int action;

    private String id;
    private String message;
    private String itemType;
    private String type;
    private String messageType;

    private ItemMessage itemMessage = null;
    private ConfigurationMessage configurationMessage = null;


    public ExportMessage(String id, String message, int action, String itemType, String type, int jobID) {
        this.setID(id);
        this.setMessage(message);
        this.setAction(action);
        this.setItemType(itemType);
        this.setType(type);
        this.setJobID(jobID);
        if (type.equals(EXPORT_JSON_KEY_ITEM) && action != MYSQL_ACTION_DELETE)
            this.setMessageType(fetchMessageType(message));
        else
            this.setMessageType(EXPORT_JSON_KEY_ITEM);
    }

    public String getMessage() {
        return message;
    }

    private void setMessage(String message) {
        if (message == null) {
            message = "";
        }
        this.message = message;
    }

    public String getType() {
        return type;
    }

    private void setType(String type) {
        this.type = type;
    }

    public int getAction() {
        return action;
    }

    private void setAction(int action) {
        this.action = action;
    }

    public String getID() {
        return id;
    }

    private void setJobID(int jobID) {
        this.jobID = jobID;
    }

    public int getJobID() {
        return jobID;
    }

    private void setID(String id) {
        this.id = id;
    }

    public String getItemType() {
        return itemType;
    }

    private void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public ItemMessage getItemMessage() {
        if (EXPORT_JSON_KEY_ITEM.equals(type) || TYPE_FILE_CONTENT.equals(type)) {
            itemMessage = new ItemMessage(message, id, action, messageType, jobID);
            return itemMessage;
        }
        return new ItemMessage();
    }

    public ConfigurationMessage getConfigurationMessage() {
        if (type.equals(TYPE_CONFIGURATION) || type.equals(TYPE_MAPPING)) {
            configurationMessage = new ConfigurationMessage(message, type, id, action, jobID);
            return configurationMessage;
        }
        return new ConfigurationMessage();
    }

    public Message getMessageData() {
        return new Message(message, id, action, messageType, jobID);
    }

    public OperationMessage getOperationData() {
        return new OperationMessage(message, action, jobID, getMessageType());
    }

    public String getMessageType() {
        return messageType;
    }

    private void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    private String fetchMessageType(String message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message);
        String messageType = EXPORT_JSON_KEY_ITEM;
        if (jsonObject != null) {
            if (jsonObject.containsKey(EXPORT_JSON_KEY_RECORD)) {
                messageType = EXPORT_JSON_KEY_RECORD;
            } else if (jsonObject.containsKey(EXPORT_JSON_KEY_DEBUG_MODE)) {
                messageType = EXPORT_JSON_KEY_DEBUG_MODE;
            } else if (jsonObject.containsKey(EXPORT_JSON_KEY_REINDEX)) {
                messageType = EXPORT_JSON_KEY_REINDEX;
            }
        }
        return messageType;
    }
}
