package com.exportstaging.api.domain;

import com.exportstaging.api.dao.ItemAPIDAOImpl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
* Serialize plain old Java object (POJO) of Item which contain all information related to a CONTENTSERV Item . 
*
*/
public class Reference implements Serializable {

    private static final long serialVersionUID = 1L;
    private long itemID;
    private long attributeID;
    private String itemType;
    private long languageID;
    private long referenceID;
    private long sortOrder;
    private long targetID;
    private String targetType;
    private long configurationID;
    private Map<Long, String> valueMapListReference;
    private Map<Long, String> formattedValueMapListReference;
    private List<Long> attributeIDs = new ArrayList<>();
    private String externalKey;

    /**
     * Returns Class of Attribute.(ContentServ PdmconfigurationID )
     *
     * @return long: ContentServ class ID of the Attribute.
     */
    public long getConfigurationID() {
        return configurationID;
    }

    public void setConfigurationID(long configurationID) {
        this.configurationID = configurationID;
    }

    /**
     * Returns External Key of Reference.(ContentServ External Key )
     *
     * @return String: ContentServ External key of the Reference.
     */
    public String getExternalKey() {
        return externalKey;
    }

    public void setExternalKey(String externalKey) {
        this.externalKey = externalKey;
    }

    /**
     * Returns ID of Item.(ContentServ Item ID)
     *
     * @return long: ContentServ ID of the Item.
     */
    public long getItemID() {
        return itemID;
    }

    /**
     * Returns ID of reference Attribute.(ContentServ Attribute ID)
     *
     * @return long: ContentServ ID of the reference Attribute.
     */
    public long getAttributeID() {
        return attributeID;
    }

    /**
     * Returns Type of Item .(e.g. Whether item is PIM or MAM)
     *
     * @return String: Type of Item.
     */
    public String getItemType() {
        return itemType;
    }

    /**
     * Returns the LanguageID of the current Item.(the current attribute data
     * language eg. 1 for english, 2 for german )
     *
     * @return long: LanguageID of the Item.
     */
    public long getLanguageID() {
        return languageID;
    }

    /**
     * Returns the CONTENTSERV reference id of the reference attribute which is
     * use to manage reference in the contentserv.
     *
     * @return integer: CONTENTSERV reference id of the reference attribute.
     */
    public long getReferenceID() {
        return referenceID;
    }

    /**
     * Returns the CSort Orderof the reference attribute.
     *
     * @return long: Sort Order of the reference attribute.
     */
    public long getSortOrder() {
        return sortOrder;
    }

    /**
     * Returns the Item ID for the Item which is referenced in the attribute.
     *
     * @return long: referenced Item ID .
     */
    public long getTargetID() {
        return targetID;
    }

    /**
     * Returns the type for the Item which is referenced in the attribute.(e.g.
     * PIM or MAM)
     *
     * @return String: referenced Item type.
     */
    public String getTargetType() {
        return targetType;
    }

    public void setItemID(long itemID) {
        this.itemID = itemID;
    }

    public void setAttributeID(long attributeID) {
        this.attributeID = attributeID;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public void setLanguageID(long languageID) {
        this.languageID = languageID;
    }

    public void setReferenceID(long referenceID) {
        this.referenceID = referenceID;
    }

    public void setSortOrder(long sortOrder) {
        this.sortOrder = sortOrder;
    }

    public void setTargetID(long targetID) {
        this.targetID = targetID;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public Map<Long, String> getValueMapListReference() {
        return valueMapListReference;
    }

    public Map<Long, String> getFormattedValueMapListReference() {
        return formattedValueMapListReference;
    }

    public void setValueMapListReference(Map<Long, String> valueMapListReference) {
        this.valueMapListReference = valueMapListReference;
    }

    public void setFormattedValueMapListReference(Map<Long, String> formattedValueMapListReferenc) {
        this.formattedValueMapListReference = formattedValueMapListReferenc;
    }

    /**
     * Returns reference Attribute's formatted Value of the reference by
     * externalKey.
     *
     * @param externalKey: External key of the Reference
     * @return String: reference Attribute's formatted Value of the reference.
     */
    public String getFormattedValueByExternalKey(String externalKey) {
        return formattedValueMapListReference.get(ItemAPIDAOImpl.attributeExternalKey.get(getItemType()).get(externalKey));
    }

    /**
     * Returns reference Attribute's Value of the reference by externalKey.
     *
     * @param externalKey: External key of the Reference
     * @return String: reference Attribute's Value of the reference.
     */
    public String getValueByExternalKey(String externalKey) {
        return valueMapListReference.get(ItemAPIDAOImpl.attributeExternalKey.get(getItemType()).get(externalKey));
    }

    /**
     * Returns reference Attribute's formatted Value of the reference by ID.
     *
     * @param attributeId :long Attribute ID
     * @return String: reference Attribute's formatted Value of the reference.
     */
    public String getFormattedValueById(long attributeId) {
        return formattedValueMapListReference.get(attributeId);
    }

    /**
     * Returns reference Attribute's Value of the reference by ID.
     *
     * @param attributeId :long Attribute ID
     * @return String: reference Attribute's Value of the reference.
     */
    public String getValueById(long attributeId) {
        return valueMapListReference.get(attributeId);
    }

    /**
     * @return List of all the attributes assigned to the current item
     */
    public List<Long> getAttributeIDs() {
        return attributeIDs;
    }

    public void setAttributeIDs(List<Long> attributeIDs) {
        this.attributeIDs = attributeIDs;
    }
}
