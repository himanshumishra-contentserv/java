package com.exportstaging.domain;

import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.*;

import static com.exportstaging.common.ExportMiscellaneousUtils.*;

/**
 * Operation Message Class for easy transformation of Operation Message.<br>
 * Used for Operations of Initial Export, Adding/Removing Item Types.
 */
public class OperationMessage extends Message {
    public static final String OPERATION_INITIALIZE = "initialize";
    public static final String OPERATION_ITEM_TYPE_ADDED = "itemTypeAdded";
    public static final String OPERATION_ITEM_TYPE_REMOVED = "itemTypeRemoved";

    private String operationType;
    private List<String> itemTypes;
    private List<String> affectedItemIds;
    private List<String> changedData;
    private List<String> searchableFieldIds;
    private Map<String, List<String>> cassandraMVDetails = new HashMap();

    /**
     * Constructor for OperationMessage
     *
     * @param messageJSON Comma-separated ItemTypes to handle for the given operation
     * @param action      Operation ActionID
     * @throws ExportStagingException when unsupported actionID is provided. Currently used actionIDs 1, 2, 4 for added, removed and initial operations respectively
     */
    public OperationMessage(String messageJSON, int action, int jobID, String messageType) {
        setJobId(jobID);
        setItemTypes(messageJSON);
        if (EXPORT_JSON_KEY_DEBUG_MODE.equals(messageType)) {
            setOperationType(EXPORT_JSON_KEY_DEBUG_MODE);
        }else if (EXPORT_JSON_KEY_REINDEX.equals(messageType)){
            setOperationType(EXPORT_JSON_KEY_REINDEX);
            setAffectedItemIDs(messageJSON);
            setSearchableFieldIds(messageJSON);
            //setChangedData(messageJSON);
        }
        else {
            if (action == 4) {
                setOperationType(OPERATION_INITIALIZE);
            } else if (action == 1) {
                setOperationType(OPERATION_ITEM_TYPE_ADDED);
            } else if (action == 2) {
                setOperationType(OPERATION_ITEM_TYPE_REMOVED);
            } else {
                System.out.println("Unsupported Operation. ActionID " + action + " is not valid for Operation Message");
            }
        }
    }

    private void setItemTypes(String message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message);
        Object object = null;
        JSONObject cassandraMVFields = null;
        if (jsonObject.containsKey(EXPORT_JSON_KEY_ITEM)) {
            object = jsonObject.get(EXPORT_JSON_KEY_ITEM);
            cassandraMVFields = (JSONObject) jsonObject.get("MVFields");
        } else if (jsonObject.containsKey(EXPORT_JSON_KEY_DEBUG_MODE)) {
            object = jsonObject.get(EXPORT_JSON_KEY_DEBUG_MODE);
        }else if (jsonObject.containsKey(EXPORT_JSON_KEY_REINDEX)) {
            jsonObject = (JSONObject) jsonObject.get(EXPORT_JSON_KEY_REINDEX);
            object     = jsonObject.get("ItemType");
        }

        if (object != null) {
            itemTypes = new ArrayList<>(Arrays.asList(String.valueOf(object).replaceAll(" ", "").split(",")));
            if (cassandraMVFields != null) {
                cassandraMVDetails.clear();
                cassandraMVDetails = ExportMiscellaneousUtils.convertToMap(cassandraMVFields);
            }
        }
    }

    private void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    /**
     * Gives information of the Operation performed
     *
     * @return Operation Information<br> InitialExport: "initialize"<br> Item Type added: "itemTypeAdded"<br> Item Type Removed: "itemTypeRemoved"
     */
    public String getOperationType() {
        return operationType;
    }

    /**
     * Gives the list of item types affected in this operation
     *
     * @return List of item types and record types
     */
    public List<String> getItemTypes() {
        return itemTypes;
    }


    /**
     * provides details of cassandra materialized view fields
     *
     * @return map of item type and list of MV fields.
     */
    public Map<String, List<String>> getCassandraMVDetails()
    {
        ExportMiscellaneousUtils.getDefaultCassandraMVFields(cassandraMVDetails);
        return cassandraMVDetails;
    }


    /**
     * Sets the list of item Ids affected in this operation
     *
     * @return List of item types and record types
     */
    private void setAffectedItemIDs(String message) {
        this.affectedItemIds = null;
        JSONArray object = null;
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message);
        if(jsonObject.containsKey(EXPORT_JSON_KEY_REINDEX)) {
            jsonObject = (JSONObject) jsonObject.get(EXPORT_JSON_KEY_REINDEX);
            object = (JSONArray) jsonObject.get("AffectedItemIDs");
            if (object != null) {
                List<String> list = new ArrayList<String>();
                for(int i = 0; i < object.size(); i++){
                    list.add((String) object.get(i));
                }
                this.affectedItemIds = list;
            }
        }
    }


    /**
     * Gives the list of item Ids affected in this operation
     *
     * @return List of item types and record types
     */
    public List<String> getAffectedItemIDs() {
        return this.affectedItemIds;
    }

    private void setChangedData(String message) {
        this.changedData = null;
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message);

        if (jsonObject.containsKey(EXPORT_JSON_KEY_REINDEX)) {
            jsonObject = (JSONObject) jsonObject.get(EXPORT_JSON_KEY_REINDEX);
            jsonObject = (JSONObject) JSONValue.parse((String) jsonObject.get("ChangedData"));
            if (jsonObject != null) {
                Set attributeIds = jsonObject.keySet();
                List<String> list = new ArrayList<String>();

                for (Object key : attributeIds) {
                    list.add((String) key);
                }

                this.changedData = list;
            }
        }
    }


    public List<String> getChangedData()
    {
        return this.changedData;
    }


    /**
     * Set the searchable fields from reindex operation message.
     */
    private void setSearchableFieldIds(String message)
    {
        searchableFieldIds = null;
        JSONArray  object     = null;
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message);
        if (jsonObject != null && jsonObject.containsKey(EXPORT_JSON_KEY_REINDEX)) {
            jsonObject = (JSONObject) jsonObject.get(EXPORT_JSON_KEY_REINDEX);
            if (jsonObject != null) {
                object = (JSONArray) jsonObject.get(ES_FIELDS_SEARCHABLE);
                if (object != null) {
                    List<String> list = new ArrayList<String>();
                    for (int i = 0; i < object.size(); i++) {
                        list.add(String.valueOf(object.get(i)));
                    }
                    searchableFieldIds = list;
                }
            }
        }
    }


    /**
     * List of searchable field ids
     *
     * @return List of searchable field ids
     */
    public List<String> getSearchableFieldIds()
    {
        return searchableFieldIds;
    }
}
