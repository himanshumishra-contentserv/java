package com.exportstaging.api.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialize plain old Java object (POJO) of Attribute which contain all
 * information related to a CONTENTSERV Attribute .
 */
public class Attribute implements Serializable {

    private static final long serialVersionUID = 1L;
    private String id;
    private String label;
    private String externalKey;
    private String isClass;
    private String languageDependency;
    private String paneTitle;
    private String sectionTitle;
    private String defaultValue;
    private String description;
    private String inherited;
    private String isFolder;
    private String type;
    private String typeID;
    private String sortOrder;
    private String tags;
    @JsonIgnore
    private String propertyValue;
    @JsonIgnore
    Map<String, String> param = null;
    Map<String, String> fieldValueMapping = null;
    List<Attribute> attribute;

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    /**
     * Return the 0,1,2,3 if its is Folder, Simple attribute, reference, table type attributes
     * respectively.
     *
     * @return String : ID
     */
    public String getTypeID() {
        return typeID;
    }

    /**
     * Set the attribute type 0, 1, 2, 3 if it is Folder, Simple attribute, reference, table type attributes
     * respectively.
     *
     * @param typeID String attribute type ID
     */
    public void setTypeID(String typeID) {
        this.typeID = typeID;
    }

    /**
     * Returns ID of Attribute.(ContentServ Attribute ID)
     *
     * @return String: ContentServ ID of the Attribute.
     */
    public String getId() {
        return id;
    }

    /**
     * Set Attribute ID
     *
     * @param iD String
     */
    public void setId(String iD) {
        this.id = iD;
    }

    /**
     * Returns Label of the Attribute.
     *
     * @return String: Label of the Attribute.
     */
    public String getLabel() {
        return label;
    }

    /**
     * Set Attribute Label
     *
     * @param label Attribute Label
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * Returns External Key of Attribute which is use for manage attribute from
     * outside of CONTENTSERV.
     *
     * @return String: External Key of the Attribute.
     */
    public String getExternalKey() {
        return externalKey;
    }

    /**
     * Set Attribute ExternalKey
     *
     * @param externalKey Attribute ExternalKey
     */
    public void setExternalKey(String externalKey) {
        this.externalKey = externalKey;
    }

    /**
     * check whether attributes is class or not which can contain other attributes
     * as its child.
     *
     * @return String: IsClass of the Attribute.
     */
    public String getIsClass() {
        return isClass;
    }

    /**
     * Set 1 if it is a class otherwise 0.
     *
     * @param isClass 1 or 0
     */
    public void setIsClass(String isClass) {
        this.isClass = isClass;
    }

    /**
     * Returns Language Dependency of Attribute (Whether the attribute has
     * language dependant values, if attribute is language dependant it can store
     * different values in different languages).
     *
     * @return String: Language Dependency of the Attribute.
     */
    public String getLanguageDependency() {
        return languageDependency;
    }

    /**
     * Set 1 if want to make language dependent otherwise 0.
     *
     * @param languageDependency 1 or 0
     */
    public void setLanguageDependency(String languageDependency) {
        this.languageDependency = languageDependency;
    }

    /**
     * Returns the pane title of the attribute. Pane title can contain group of
     * attributes which is belongs to same categories.
     *
     * @return String: PaneTitle of the Attribute.
     */
    public String getPaneTitle() {
        return paneTitle;
    }

    /**
     * Set Attribute Pane title.
     *
     * @param paneTitle Pane title.
     */
    public void setPaneTitle(String paneTitle) {
        this.paneTitle = paneTitle;
    }

    /**
     * Returns Section Title of Attribute.Section Title can contain group of
     * attributes which is belongs to same categories.
     *
     * @return String: Section Title of the Attribute.
     */
    public String getSectionTitle() {
        return sectionTitle;
    }

    /**
     * Set Attribute section title.
     *
     * @param sectionTitle Section title.
     */
    public void setSectionTitle(String sectionTitle) {
        this.sectionTitle = sectionTitle;
    }

    /**
     * Returns Default Value of Attribute(user can set default value of the
     * attribute which can be used if attribute does not contain any unformatted
     * and formatted value).
     *
     * @return String: Default Value of the Attribute.
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Set Attribute default value.
     *
     * @param defaultValue attribute default value
     */
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * Returns Description of Attribute(additional description related to the
     * attribute)
     *
     * @return String: Description of the Attribute.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets Description of Attribute(additional description related to the
     * attribute)
     *
     * @param description - the Description of Attribute to be set.
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Returns Inheritance Type of Attribute (Whether the Attribute value may be
     * inherited from the parent items).
     *
     * @return String: Inheritance Type of the Attribute.
     */
    public String getInherited() {
        return inherited;
    }

    /**
     * Sets Inheritance Type of Attribute
     *
     * @param inherited - Inheritance Type of Attribute
     */
    public void setInherited(String inherited) {
        this.inherited = inherited;
    }

    /**
     * Returns 1 if it is Folder, else returns 0
     *
     * @return isFolder : String value of IsFolder
     */
    public String getIsFolder() {
        return isFolder;
    }

    /**
     * Sets the Folder
     *
     * @param isFolder - value 1, if it is Folder, else returns 0
     */
    public void setIsFolder(String isFolder) {
        this.isFolder = isFolder;
    }

    /**
     * Returns Type of Attribute (e.g. Whether attribute is
     * caption,number,selection type etc.)
     *
     * @return String: Type of the Attribute.
     */
    public String getType() {
        return type;
    }

    /**
     * Set attribute type
     *
     * @param type - attribute type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Returns Sort Order of the Attribute
     *
     * @return String:Sort Order of Attribute
     */
    public String getSortOrder() {
        return sortOrder;
    }

    /**
     * Sets the Sort Order of the Attribute
     *
     * @param sortOrder - Sort Order of the Attribute
     */
    public void setSortOrder(String sortOrder) {
        this.sortOrder = sortOrder;
    }

    /**
     * Returns Tag ID(s) of Attribute
     *
     * @return String : Tag ID(s) of Attribute
     */
    public String getTags() {
        return tags;
    }

    /**
     * Set Tag ID(s) of Attribute
     *
     * @param tags String
     */
    public void setTags(String tags) {
        this.tags = tags;
    }

    /**
     * Returns Property value of Attribute
     *
     * @return String : Property value
     */
    public String getPropertyValue() {
        return propertyValue;
    }

    /**
     * Sets property value of Attribute
     *
     * @param propertyValue : Property value
     */
    public void setPropertyValue(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    public Map<String, String> getParam() {
        return param;
    }

    /**
     * Set Map of Params of Attribute.
     *
     * @return param : HashMap Params of the Attribute.
     */
    public Map<String, String> setParamIfNull() {
        if (param == null) {
            param = new HashMap<String, String>();
        }
        return this.param;
    }

    /**
     * Set all the attribute Param Value in map as key pair.
     *
     * @param param All Param values.
     */
    public void setParam(Map<String, String> param) {
        this.param = param;
    }

    public String getParamA() {
        return this.getValue("ParamA");
    }

    public String getParamB() {
        return this.getValue("ParamB");
    }

    public String getParamC() {
        return this.getValue("ParamC");
    }

    public String getParamD() {
        return this.getValue("ParamD");
    }

    public String getParamE() {
        return this.getValue("ParamE");
    }

    public String getParamF() {
        return this.getValue("ParamF");
    }

    public String getParamG() {
        return this.getValue("ParamG");
    }

    public String getParamH() {
        return this.getValue("ParamH");
    }

    public String getParamI() {
        return this.getValue("ParamI");
    }

    public String getParamJ() {
        return this.getValue("ParamJ");
    }

    public List<Attribute> getAttribute() {
        return attribute;
    }

    public void setAttribute(List<Attribute> attribute) {
        this.attribute = attribute;
    }

    /**
     * Returns Property Value of Attribute.(additional information related to the
     * attribute)
     *
     * @return propertyValueMap : HashMap Property Value of the Attribute.
     * @throws ParseException       If any type of Exception will throw Handle by this ParseException Exception
     * @throws IOException          If any type of Exception will throw Handle by this IOException Exception
     * @throws JsonMappingException If any type of Exception will throw Handle by this JsonMappingException Exception
     * @throws JsonParseException   If any type of Exception will throw Handle by this JsonParseException Exception
     */

    @SuppressWarnings("unchecked")
    public HashMap<String, String> getPropertyValues() throws ParseException, IOException {
        HashMap<String, String> propertyValueMap = new HashMap<String, String>();
        if (this.propertyValue != null && !this.propertyValue.trim().isEmpty()) {
            propertyValueMap = (HashMap<String, String>) new ObjectMapper().readValue(propertyValue,
                    Map.class);

        }
        return propertyValueMap;
    }

    /**
     * Returns Value of corresponding Key. It will return value of attribute
     *
     * @param key String key
     * @return param : String Value of the given Key
     */

    @JsonIgnore
    public String getValue(String key) {
        String fieldValue = fieldValueMapping.get(key);
        if (fieldValue != null) {
            return fieldValue;
        }
        return null;
    }

    public void setFieldValueMapping(HashMap<String, String> fieldValueMapping) {
        this.fieldValueMapping = fieldValueMapping;
    }

}
