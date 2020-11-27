package com.exportstaging.api.domain;

import com.exportstaging.api.dao.ItemAPIDAOImpl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
* plain old Java object (POJO) of Sub table  which contain all information related to a CONTENTSERV Sub table row. 
*
*/
public class Subtable implements Serializable {

    private static final long serialVersionUID = 1L;
    private long itemID;
    private long attributeID;
    private long csItemTableID;
    private long languageID;
    private long sortOrder;
    private String itemType;
    private long subItemID;  // this is ItemID or ParentRowID
    private String subtableIDs; // comma separated list of nested subtable ids
    private String externalKey;
    private String referenceIDs; // comma separated list of reference ids located inside a subtable
    private long configurationID;
    private ExportValues exportValues = new ExportValues();
    private Map<Long, String> valueMapListSubTable;
    private Map<Long, String> formattedValueMapListSubTable;
    private List<Long> attributeIDs = new ArrayList<>();
    private Map<String, ArrayList> referenceDataMap = new HashMap<>();

    /**
     * Returns ExportValue object, which will contains the lists of Reference, Subtable, AttributeValue objects
     * of default language
     * @return ExportValue
     */
    public ExportValues getExportValues() {
        return exportValues;
    }

    public void setExportValues(ExportValues exportValues) {
        this.exportValues = exportValues;
    }

    public long getConfigurationID() {
        return configurationID;
    }

    public void setConfigurationID(long configurationID) {
        this.configurationID = configurationID;
    }

    /**
     * Returns External Key of Attribute.(ContentServ External Key )
     *
     * @return String: ContentServ ID of the Item.
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
     * @return String: ContentServ ID of the Item.
     */
    public long getItemID() {
        return itemID;
    }

    /**
     * Returns ID of table Attribute.(ContentServ Attribute ID)
     *
     * @return String: ContentServ ID of the table Attribute.
     */
    public long getAttributeID() {
        return attributeID;
    }

    /**
     * Returns row ID of table Attribute.(ContentServ row ID)
     *
     * @return String: Row ID of the table Attribute.
     */
    public long getCSItemTableID() {
        return csItemTableID;
    }

    /**
     * Returns the LanguageID of the current Item.(the current attribute data
     * language eg. 1 for english, 2 for german )
     *
     * @return String: LanguageID of the Item.
     */
    public long getLanguageID() {
        return languageID;
    }

    /**
     * Returns the sort order of the table attribute.
     *
     * @return String: sort order of the table attribute.
     */
    public long getSortOrder() {
        return sortOrder;
    }

    /**
     * Returns Item Type of Item.
     *
     * @return String: Type of the Item.
     */
    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public void setItemID(long itemID) {
        this.itemID = itemID;
    }

    public void setAttributeID(long attributeID) {
        this.attributeID = attributeID;
    }

    public void setCSItemTableID(long cSItemTableID) {
        this.csItemTableID = cSItemTableID;
    }

    public void setLanguageID(long languageID) {
        this.languageID = languageID;
    }

    public void setSortOrder(long sortOrder) {
        this.sortOrder = sortOrder;
    }

    public void setValueMapListSubTable(Map<Long, String> valueMapListSubTable) {
        this.valueMapListSubTable = valueMapListSubTable;
    }

    public void setFormattedValueMapListSubTable(Map<Long, String> formattedValueMapListSubTable) {
        this.formattedValueMapListSubTable = formattedValueMapListSubTable;
    }

    /**
     * Returns table Attribute's formatted Value of the table by externalKey.
     *
     * @param externalKey : string externalKey field.
     * @return String: table Attribute's formatted Value of the table.
     */
    public String getFormattedValueByExternalKey(String externalKey) {
        return formattedValueMapListSubTable.get(ItemAPIDAOImpl.attributeExternalKey.get(getItemType()).get(externalKey));
    }

    /**
     * Returns table Attribute's Value of the table by externalKey.
     *
     * @param externalKey : string externalKey field.
     * @return String: table Attribute's Value of the table.
     */
    public String getValueByExternalKey(String externalKey) {
        return valueMapListSubTable.get(ItemAPIDAOImpl.attributeExternalKey.get(getItemType()).get(externalKey));
    }

    /**
     * Returns table Attribute's formatted Value of the table by ID.
     *
     * @param attributeId : long attribute ID.
     * @return String: table Attribute's formatted Value of the table.
     */
    public String getFormattedValueById(long attributeId) {
        return formattedValueMapListSubTable.get(attributeId);
    }

    /**
     * Returns table Attribute's Value of the table by ID.
     *
     * @param attributeId : long attribute ID.
     * @return String: table Attribute's Value of the table.
     */
    public String getValueById(long attributeId) {
        return valueMapListSubTable.get(attributeId);
    }

    /**
     * List of attributes which assigned to table.
     * @return List of attribute Ids.
     */
    public List<Long> getAttributeIDs() {
        return attributeIDs;
    }

    public void setAttributeIDs(List<Long> attributeIDs) {
        this.attributeIDs = attributeIDs;
    }

    public void setSubItemID(long subItemID) {
        this.subItemID = subItemID;
    }

    /**
     * Returns Id of item which contains table data
     * @return item id or table id in case of nested table
     */
    public long getSubItemID() {
        return subItemID;
    }

    public void setSubtableIDs(String subtableIDs) {
        this.subtableIDs = subtableIDs;
    }

    /**
     * Returns ids of table attribute which assigned to item table.
     * @return Comma separated list of table attributes.
     */
    public String getSubtableIDs() {
        return subtableIDs;
    }

    /**
     * Returns ids of reference attribute which assigned to item table.
     * @return Comma separated list of reference attributes.
     */
    public String getReferenceIDs() {
        return referenceIDs;
    }

    public void setReferenceIDs(String referenceIDs) {
        this.referenceIDs = referenceIDs;
    }

    public Map<String, ArrayList> getReferenceDataMap() {
        return referenceDataMap;
    }

    public void setReferenceDataMap(Map<String, ArrayList> referenceDataMap) {
        this.referenceDataMap = referenceDataMap;
    }
}
