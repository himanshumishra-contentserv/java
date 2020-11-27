package com.exportstaging.domain;


import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This Class contains all the apis that is used to get the pim/mam configuration data
 * and the mapping data in proper format.
 */

public class ConfigurationMessage {
    private String JSONMessage = "";
    private JSONObject parsedConfiguration = new JSONObject();
    private Map<String, String> ConfigurationData;
    private String[] mappingMessage;
    private List<String> MappingsData;
    private String messageType = "";
    private String id;
    private String configurationType = "Mapping";
    private int action;
    private int jobID;

    public ConfigurationMessage() {
    }

    /**
     * Constructor to initialise the JSONMessage class variable and calls the parseJson() method to
     * parse the string.
     *
     * @param JSONMessage String message of the Mapping type or Configuration type
     * @param messageType Type can be either "Configuration" or "Mapping"
     * @param id          ID of the message
     * @param action      int Action of the operation.
     * @param jobID       int Job id of active script
     */
    public ConfigurationMessage(String JSONMessage, String messageType, String id, int action, int jobID) {
        setId(id);
        setJobId(jobID);
        setMessageType(messageType);
        setJSONMessage(JSONMessage);
        setAction(action);
        switch (messageType) {
            case "Mapping":
                mappingMessage = this.getJSONMessage().split(",");
                break;
            case "Configuration":
                this.parseJSON();
                break;
        }

    }

    private void parseJSON() {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(this.getJSONMessage());

        for (Object key : jsonObject.keySet()) {
            this.setConfigurationType((String) key);
            this.setParsedConfiguration((JSONObject) jsonObject.get(key));
        }
    }

    private void prepareConfigurationValues() {
        setConfigurationData(getParsedConfiguration());
    }

    private void prepareMappingValues() {
        MappingsData = new ArrayList<>();
        MappingsData = Arrays.asList(mappingMessage);
    }

    /**
     * Returns the Configuration data i.e. item, references or subtable.
     *
     * @return : ConfigurationData. ConfigurationData member contains the information
     * about the item or references or subtable attributes .
     */
    public Map<String, String> getConfigurationData() {
        prepareConfigurationValues();
        return ConfigurationData;
    }


    /**
     * Returns the Mapping data i.e. class
     *
     * @return : MappingData. class related information
     */
    public List<String> getMappingData() {
        prepareMappingValues();
        return MappingsData;
    }

    private void setConfigurationData(Map<String, String> configurationData) {
        ConfigurationData = configurationData;
    }

    /**
     * Returns the JSONObject of the Configuration message
     *
     * @return JSONObject of Configuration Message which can be used to get the configuration data
     */
    public JSONObject getParsedConfiguration() {
        return parsedConfiguration;
    }

    private void setParsedConfiguration(JSONObject parsedItem) {
        this.parsedConfiguration = parsedItem;
    }

    private String getJSONMessage() {
        return JSONMessage;
    }

    private void setJSONMessage(String JSONMessage) {
        this.JSONMessage = JSONMessage;
    }

    /**
     * Provides information of the type of data this message holds
     *
     * @return String Type of the message, if it is configuration or mapping.
     */
    public String getMessageType() {
        return messageType;
    }


    private void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    /**
     * Returns the ID of the message
     *
     * @return String ID of the message
     */
    public String getId() {
        return id;
    }

    public int getJobId() {
    	return this.jobID;
    }

    private void setId(String id) {
        this.id = id;
    }

    protected void setJobId(int jobID) {
        this.jobID = jobID;
    }

    /**
     * Provides the Information about the Type of configuration i.e Item or Reference or Subtable
     *
     * @return String Configuration Type
     */
    public String getConfigurationType() {
        return configurationType;
    }

    private void setConfigurationType(String configurationType) {
        this.configurationType = configurationType;
    }

    /**
     * Returns the Message action. If the message is of type create, update or delete
     *
     * @return int action of the message
     */
    public int getAction() {
        return action;
    }

    protected void setAction(int action) {
        this.action = action;
    }
}
