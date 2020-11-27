package com.exportstaging.domain;

import com.exportstaging.common.ExportMiscellaneousUtils;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This Class contains all the apis that is used to get the item data
 * and the mapping data in proper format.
 * <b> ItemMessage class provides the apis to get the item related data like item, references and subtable</b>
 */

public class ItemMessage extends Message {
    private String JSONMessage = null;
    private JSONArray parsedReference = null;
    private JSONArray parsedSubtable = null;
    private Map<String, List<String>> bigDataAttributes = new TreeMap<>();
    private Map<Integer, Map<Integer, List<Map>>> ReferenceData;
    private Map<Integer, Map<Integer, List<Map>>> SubtableData;
    private Set<String> languageIds = new HashSet<>();

    protected static Logger logger;

    public ItemMessage() {

    }

    /**
     * Constructor to initialise the JSONMessage class variable and calls the parseJson() method to
     * parse the string.
     *
     * @param JSONMessage String message of the item
     * @param id          Message ID
     * @param action      int Action of the Operation performed
     * @param messageType Is the Message is for Item or Record
     * @param jobID       int active script job id
     */
    public ItemMessage(String JSONMessage, String id, int action, String messageType, int jobID) {
        setJobId(jobID);
        setId(id);
        setJSONMessage(JSONMessage);
        setAction(action);
        setMessageType(messageType);
        this.parseJSON();
    }

    private void parseJSON() {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(this.getJSONMessage());

        JSONArray itemMessage = (JSONArray) jsonObject.get(getMessageType());
        //convert language independent attribute data to original form
        if (getMessageType().equals(ExportMiscellaneousUtils.EXPORT_JSON_KEY_ITEM)) {
            itemMessage = mergeItemLanguageIndependentAttrData(itemMessage);
        }
        setItemMessage(itemMessage);

        //convert reference language independent attribute data to original form
        JSONArray parsedReference = (JSONArray) jsonObject.get("Reference");
        parsedReference = mergeRefLanguageIndependentAttrData(parsedReference);
        this.setParsedReference(parsedReference);

        JSONArray parsedSubtable = (JSONArray) jsonObject.get("Subtable");
        this.setParsedSubtable((parsedSubtable == null) ? new JSONArray() : parsedSubtable);

        this.setBigDataAttributes(buildBigAttributeData(jsonObject));
    }


    /**
     * Prepare map of language id and list of big data attributes
     *
     * @param jsonObject Json object from which we can get the details
     *
     * @return map of language id and list of big data attributes
     */
    private Map<String, List<String>> buildBigAttributeData(JSONObject jsonObject)
    {
        Map<String, List<String>> map = new HashMap<>();
        try {
            Object bigDataAttributes = jsonObject.get("_BigDataAttributes");
            if (bigDataAttributes instanceof JSONArray) {
                JSONArray bigDataAttributesData = (JSONArray) bigDataAttributes;
                if (bigDataAttributesData != null) {
                    for (int i = 0; i < bigDataAttributesData.size(); i++) {
                        if (bigDataAttributesData.get(i) != null) {
                            if (map.get("0") != null) {
                                HashSet<String> attributes = new HashSet<>(map.get("0"));
                                attributes.addAll((List<String>) bigDataAttributesData.get(i));
                                map.put(Integer.toString(i), new ArrayList<String>(attributes));
                                continue;
                            }
                            map.put(Integer.toString(i), (List<String>) bigDataAttributesData.get(i));
                        }
                        else if (map.get("0") != null) {
                            map.put(Integer.toString(i), map.get("0"));
                        }
                    }
                }
            }
            else if (bigDataAttributes instanceof JSONObject) {
                JSONObject                   jsonObj = (JSONObject) bigDataAttributes;
                Set<Map.Entry<String, List>> set     = jsonObj.entrySet();
                set.stream().forEach((Map.Entry<String, List> entry) -> map.put(entry.getKey(), entry.getValue()));
            }
        } catch (Exception exception) {
            logger.error("Exception While Processing Big Data Attributes. Details Message :" + exception.getMessage());
        }
        return map;
    }


    private JSONArray mergeItemLanguageIndependentAttrData(JSONArray itemMessage) {
        JSONArray itemJsonArray = new JSONArray();
        if (itemMessage != null) {
            JSONObject languageIndAttrData = new JSONObject();
            for (Object anItemMessage : itemMessage) {
                JSONObject languageData = (JSONObject) anItemMessage;
                if (languageData.get(ExportMiscellaneousUtils.EXPORT_FIELD_LANGUAGEID).equals("0")) {
                    languageIndAttrData = languageData;
                    languageIndAttrData.remove(ExportMiscellaneousUtils.EXPORT_FIELD_LANGUAGEID);
                    languageIndAttrData.remove("LanguageShortName");
                } else {
                    languageData.putAll(languageIndAttrData);
                    itemJsonArray.add(languageData);
                    //Use for iterating language wise reference data
                    languageIds.add((String) languageData.get(ExportMiscellaneousUtils.EXPORT_FIELD_LANGUAGEID));
                }
            }
        }

        return itemJsonArray;
    }

    private JSONArray mergeRefLanguageIndependentAttrData(JSONArray itemMessage) {
        JSONArray referenceJsonArray = new JSONArray();
        if (itemMessage != null) {
            for (Object referenceData : itemMessage) {
                JSONObject languageData = (JSONObject) referenceData;
                if (languageData.get(ExportMiscellaneousUtils.EXPORT_FIELD_LANGUAGEID).equals("0")) {
                    for (String languageId : languageIds) {
                        JSONObject referenceLangData = new JSONObject(languageData);
                        referenceLangData.put(ExportMiscellaneousUtils.EXPORT_FIELD_LANGUAGEID, languageId);
                        referenceJsonArray.add(referenceLangData);
                    }
                } else {
                    referenceJsonArray.add(languageData);
                }
            }
        }

        return referenceJsonArray;
    }

    private void prepareReferenceValues() {
        JSONArray ReferenceObject = getParsedReference();
        Map<Integer, Map<Integer, List<Map>>> ReferenceLanguageMap = new HashMap<>();
        int LanguageID, AttributeID;
        for (Object obj : ReferenceObject) {
            if (obj instanceof JSONObject) {
                JSONObject ReferenceJSONObj = (JSONObject) obj;
                LanguageID = Integer.parseInt((String) ReferenceJSONObj.get("LanguageID"));
                AttributeID = Integer.parseInt((String) ReferenceJSONObj.get("AttributeID"));
                ReferenceLanguageMap.putIfAbsent(LanguageID, new HashMap<>());
                ReferenceLanguageMap.get(LanguageID).putIfAbsent(AttributeID, new ArrayList<>());
                ReferenceLanguageMap.get(LanguageID).get(AttributeID).add(ReferenceJSONObj);
            }
        }
        setReferenceData(ReferenceLanguageMap);
    }

    private void prepareSubtableValues() {
        JSONArray SubtableObject = getParsedSubtable();
        Map<Integer, Map<Integer, List<Map>>> SubtableLanguageMap = new HashMap<>();
        int LanguageID, AttributeID;
        for (Object obj : SubtableObject) {
            if (obj instanceof JSONObject) {
                JSONObject SubtableJSONObj = (JSONObject) obj;
                LanguageID = Integer.parseInt((String) SubtableJSONObj.get("LanguageID"));
                AttributeID = Integer.parseInt((String) SubtableJSONObj.get("AttributeID"));
                SubtableLanguageMap.putIfAbsent(LanguageID, new HashMap<>());
                SubtableLanguageMap.get(LanguageID).putIfAbsent(AttributeID, new ArrayList<>());
                SubtableLanguageMap.get(LanguageID).get(AttributeID).add((HashMap) obj);
            }
        }
        setSubtableData(SubtableLanguageMap);
    }


    /**
     * Returns the references Data for the item
     *
     * @return ReferenceData: Map for the references data for the item .
     */
    public Map<Integer, Map<Integer, List<Map>>> getReferenceData() {
        prepareReferenceValues();
        return ReferenceData;
    }

    /**
     * Returns the subtable Data for the item
     *
     * @return SubtableData : Map for the subtable data for the item .
     */
    public Map<Integer, Map<Integer, List<Map>>> getSubtableData() {
        prepareSubtableValues();
        return SubtableData;
    }

    private String getJSONMessage() {
        return JSONMessage;
    }

    private void setJSONMessage(String JSONMessage) {
        this.JSONMessage = JSONMessage;
    }

    /**
     * Returns the reference data for the Item
     *
     * @return Parsed JSONArray object which can be iterated to get Reference data
     */
    public JSONArray getParsedReference() {
        return parsedReference;
    }

    private void setParsedReference(JSONArray parsedReference) {
        this.parsedReference = parsedReference;
    }

    public Map<String, List<String>> getBigDataAttributes() {
        return bigDataAttributes;
    }

    private void setBigDataAttributes(Map<String, List<String>> bigDataAttributes) {
        this.bigDataAttributes = bigDataAttributes;
    }

    /**
     * Returns the subtable data for the Item
     *
     * @return Parsed JSONArray object which can be iterated to get Subtable data
     */
    public JSONArray getParsedSubtable() {
        return parsedSubtable;
    }

    private void setParsedSubtable(JSONArray parsedSubtable) {
        this.parsedSubtable = parsedSubtable;
    }

    private void setReferenceData(Map<Integer, Map<Integer, List<Map>>> referenceData) {
        ReferenceData = referenceData;
    }

    private void setSubtableData(Map<Integer, Map<Integer, List<Map>>> subtableData) {
        SubtableData = subtableData;
    }

    /**
     * The JSONString that contains the complete object information.
     *
     * @return Complete item data in json string.
     */
    public String getRawMessage() {
        return getJSONMessage();
    }
}