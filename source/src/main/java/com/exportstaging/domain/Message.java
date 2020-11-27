package com.exportstaging.domain;

import com.exportstaging.common.ExportMiscellaneousUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.HashMap;
import java.util.Map;

public class Message {
    private String id = null;
    private String messageJSON = null;
    private JSONArray parsedItem = null;
    private Map<String, Map<String, String>> ItemData;
    private int action;
    private int jobID;
    private String messageType;

    public Message() {
    }

    public Message(String messageJSON, String id, int action, String messageType, int jobID) {
        this.id = id;
        this.jobID = jobID;
        this.setMessageJSON(messageJSON);
        this.setAction(action);
        this.setMessageType(messageType);
        parseMessageJSON();
    }

    void setItemMessage(JSONArray JSONMessage) {
        this.setParsedItem(JSONMessage);
    }

    private void parseMessageJSON() {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(this.getMessageJSON());
        this.setParsedItem((JSONArray) jsonObject.get(getMessageType()));
    }


    private void prepareItemValues() {
        JSONArray itemMessage = getParsedItem();
        Map<String, Map<String, String>> itemData = new HashMap<>();
        String langID = "1";
        Map<String, String> itemObject;
        for (Object obj : itemMessage) {
            if (obj instanceof JSONObject) {
                if (((JSONObject) obj).containsKey("_States")) {
                    if (((JSONObject) obj).get("_States") instanceof JSONArray) {
                        JSONArray stateData = (JSONArray) ((JSONObject) obj).get("_States");
                        for (Object states : stateData) {
                            if (states instanceof JSONObject) {
                                Map<String, String> statesMap = (Map<String, String>) states;
                                itemData.put("States_" + statesMap.get("StateID"), statesMap);
                            }
                        }
                        ((JSONObject) obj).remove("_States");
                    }
                }
                itemObject = (HashMap<String, String>) obj;
                String languageID = itemObject.get(ExportMiscellaneousUtils.EXPORT_FIELD_LANGUAGEID);
                languageID = (languageID == null) ? langID : languageID;
                itemData.put(languageID, itemObject);
            }
        }
        setItemData(itemData);
    }


    /**
     * Provides the message data in a easy accessible format.
     * Structure is Map[LanguageID, Map[AttributeKey, AttributeValue]]
     *
     * @return Map of LanguageID against Map of Data
     */
    public Map<String, Map<String, String>> getItemData() {
        prepareItemValues();
        return ItemData;
    }

    private void setItemData(Map<String, Map<String, String>> itemData) {
        ItemData = itemData;
    }

    private String getMessageJSON() {
        return messageJSON;
    }

    private void setMessageJSON(String JSONMessage) {
        this.messageJSON = JSONMessage;
    }

    /**
     * @return Converts JSONString to {@link JSONObject}
     */
    public JSONArray getParsedItem() {
        return parsedItem;
    }

    private void setParsedItem(JSONArray parsedItem) {
        this.parsedItem = parsedItem;
    }

    /**
     * @return Returns the ID of the message
     */
    public String getId() {
        return id;
    }

    public int getJobId() {
        return this.jobID;
    }

    protected void setId(String id) {
        this.id = id;
    }

    protected void setJobId(int jobID) {
        this.jobID = jobID;
    }

    /**
     * Provides the action performed on the current message. viz is the message Created, Updated or Deleted
     *
     * @return int Action performed
     */
    public int getAction() {
        return action;
    }

    protected void setAction(int action) {
        this.action = action;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }
}
