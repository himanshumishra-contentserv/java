package com.exportstaging.api.domain;

import com.exportstaging.api.dao.ItemAPIDAOImpl;
import com.exportstaging.api.exception.ExportStagingException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Serialize plain old Java object (POJO) of Item which contain all information
 * related to a CONTENTSERV Item .
 */
@Scope("prototype")
@Component("item")
public class Item implements Serializable {

    private static final long serialVersionUID = 1L;

    private long itemID;
    private String externalKey;
    private long parentID;
    private int isFolder;
    private List<Long> classMapping;
    private long languageID;
    private String languageShortName;
    private Map<Long, String> formattedValuesMapList;
    private Map<String, String> standardValuesMapList;
    private Map<Long, String> valuesMapList;
    private List<Reference> referenceValueList;
    private List<Subtable> subTableValueList;
    private List<Long> attributeIds;
    private String itemType;
    private List<Long> stateIds;

    @Autowired
    private ItemAPIDAOImpl itemAPIDAOImpl;

    /**
     * Returns ID of Item.(ContentServ Item ID)
     *
     * @return String: ContentServ ID of the Item.
     */
    public long getItemID() {
        return itemID;
    }

    public void setItemID(long pdmArticleID) {
        this.itemID = pdmArticleID;
    }

    /**
     * Returns ParentID of the Item.(get the CONTENTSERV ID of the parant item.)
     *
     * @return String: ParentID of the Item.
     */

    public long getParentID() {
        return parentID;
    }

    public void setParentID(long parentID) {
        this.parentID = parentID;
    }

    /**
     * Returns IsFolder of the item.(return the information that current Item is
     * folder or not)
     *
     * @return String: IsFolder of the Item.
     */
    public int getIsFolder() {
        return isFolder;
    }

    public void setIsFolder(int isFolder) {
        this.isFolder = isFolder;
    }

    /**
     * Returns ClassMapping of the item which contain the attributes that are related
     * to the current Item.
     *
     * @return List: ClassMapping of the Item.
     */
    public List<Long> getClassMapping() {
        return classMapping;
    }

    public void setClassMapping(List<Long> classMapping) {
        this.classMapping = classMapping;
    }

    /**
     * Returns External Key of Item which is use for manage Item from outside of
     * CONTENTSERV.
     *
     * @return String: External Key of the Item.
     */
    public String getExternalKey() {
        return externalKey;
    }

    public void setExternalKey(String externalKey) {
        this.externalKey = externalKey;
    }

    /**
     * Returns the LanguageID of the current Item.(the current Item data language
     * eg. 1 for english, 2 for german )
     *
     * @return String: LanguageID of the Item.
     */
    public long getLanguageID() {
        return languageID;
    }

    public void setLanguageID(long languageID) {
        this.languageID = languageID;
    }

    /**
     * Returns the Language short Name of the current Item.(the current Language
     * short Name for Item data)
     *
     * @return String: Language short Name of the Item.
     */
    public String getLanguageShortName() {
        return languageShortName;
    }

    /**
     * Sets the language short name for the current item
     *
     * @param languageshortName Language Short name of the item
     */
    public void setLanguageShortName(String languageshortName) {
        this.languageShortName = languageshortName;
    }

    /**
     * Returns the SerialVersionUUID for the current object. (Note: Internal Java Implementation)
     *
     * @return serialVersionUUID
     */
    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    /**
     * @return FormattedValues for all the attributes assigned to the current item
     * Structure: Map of FormattedValue against AttributeID Map - AttributeID: FormattedValue
     */
    public Map<Long, String> getFormattedValuesMapList() {
        return formattedValuesMapList;
    }

    /**
     * @return Value for all the attributes assigned to the current item
     * Structure: Map of Value against AttributeID Map - AttributeID: Value
     */
    public Map<Long, String> getValuesMapList() {
        return valuesMapList;
    }


    public void setFormattedValuesMapList(Map<Long, String> formattedValuesMapList) {
        this.formattedValuesMapList = formattedValuesMapList;
    }

    public void setStandardValuesMapList(Map<String, String> standardValuesMapList) {
        this.standardValuesMapList = standardValuesMapList;
    }

    /**
     * @return Value for all the Standard attributes assigned to the current item
     * Structure: Map of Value against AttributeID Map - AttributeID: Value
     */
    public Map<String, String> getStandardValuesMapList() {
        return this.standardValuesMapList;
    }


    public void setValuesMapList(Map<Long, String> valuesMapList) {
        this.valuesMapList = valuesMapList;
    }

    /**
     * @return List of all the {@link Reference} attributes assigned to the current Item
     */
    public List<Reference> getReferenceValueList() {
        return referenceValueList;
    }

    /**
     * @return List of all the {@link Subtable} attributes assigned to the current Item
     */
    public List<Subtable> getSubTableValueList() {
        return subTableValueList;
    }

    public void setReferenceValueList(List<Reference> referenceValueList) {
        this.referenceValueList = referenceValueList;
    }

    public void setSubTableValueList(List<Subtable> subTableValueList) {
        this.subTableValueList = subTableValueList;
    }

    /**
     * Returns all immediate children Item IDs of the item.
     *
     * @param itemType : string Type of Item
     * @param level    : string Level
     * @return String: children item IDs of the Item.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom Exception
     */

    public Set<Long> getItemIds(int level, String itemType) throws ExportStagingException {
        return itemAPIDAOImpl.getItemChildrenById(this.itemID, level, itemType);
    }

    /**
     * Returns all custom Attribute Ids of the item.
     *
     * @return String: custom Attribute Ids of the Item.
     */
    public List<Long> getAttributeIDs() {
        return attributeIds;
    }

    public void setAttributeIDs(List<Long> attributeIds) {
        this.attributeIds = attributeIds;
    }

    /**
     * Returns custom Attribute's Formatted Value of the item by attribute ID.
     *
     * @param attributeId : int Attribute ID.
     * @return String: custom Attribute's Formatted Value of the Item.
     */
    public String getFormattedValueByID(long attributeId) {
        String formattedValue = null;
        formattedValue = formattedValuesMapList.get(attributeId);
        return formattedValue;
    }

    /**
     * Returns custom Attribute's Value of the item by attribute ID.
     *
     * @param attributeId :int ID of Attribute.
     * @return String: custom Attribute's Value of the Item.
     */
    public String getValueByID(long attributeId) {
        String value = null;
        value = valuesMapList.get(attributeId);
        return value;
    }

    /**
     * Returns List of {@link Subtable} object of the subtable attribute for the Item.
     *
     * @param attributeId : string Attribute ID.
     * @return String: subtable object of the subtable attribute for the Item.
     */
    public List<Subtable> getTableByID(int attributeId) {
        List<Subtable> subtableList = new ArrayList<Subtable>();
        for (Subtable sub : subTableValueList) {
            if (sub.getAttributeID() == attributeId) {
                subtableList.add(sub);
            }
        }
        return subtableList;
    }

    /**
     * Returns the list of all the {@link Subtable} for the provided external key assigned to the current item
     *
     * @param externalKey Subtable Attribute ExternalKey
     * @return List of {@link Subtable}
     */
    public List<Subtable> getTableByExternalKey(String externalKey) {
        List<Subtable> subtableList = new ArrayList<Subtable>();
        for (Subtable sub : subTableValueList) {
            if (!(sub.getExternalKey() == null)) {
                if (sub.getExternalKey().equals(externalKey)) {
                    subtableList.add(sub);
                }
            }
        }
        return subtableList;
    }

    /**
     * Returns the list of all the {@link Reference} for the provided external key assigned to the current item
     *
     * @param externalKey Reference Attribute ExternalKey
     * @return List of {@link Reference}
     */
    public List<Reference> getReferencesByExternalKey(String externalKey) {
        List<Reference> referenceList = new ArrayList<Reference>();
        for (Reference ref : referenceValueList) {
            if (!(ref.getExternalKey() == null)) {
                if (ref.getExternalKey().equals(externalKey)) {
                    referenceList.add(ref);
                }
            }

        }
        return referenceList;
    }

    /**
     * Returns References object of the reference attribute for the
     * Item.(reference to MAM, Reference to PIM)
     *
     * @param attributeId : int Attribute ID.
     * @return Reference: References object of the reference attribute for the Item.
     */
    public List<Reference> getReferencesByID(long attributeId) {
        List<Reference> referenceList = new ArrayList<Reference>();
        for (Reference ref : referenceValueList) {
            if (ref.getAttributeID() == (attributeId)) {
                referenceList.add(ref);
            }
        }
        return referenceList;
    }

    /**
     * Returns Item object for the current Item's parent.
     *
     * @param itemType : string Item Type.
     * @return String: Item object for the current Item's parent
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom Exception
     */

    @JsonIgnore
    public Item getParent(String itemType) throws ExportStagingException {

        return itemAPIDAOImpl.getParentData(this.parentID, itemType).getLanguagesItem().get(this.languageShortName);
    }

    /**
     * Returns custom Attribute's Value of the item by attribute ExternalKey.
     *
     * @param externalKey : string external key field.
     * @return String: custom Attribute's Value of the Item.
     */

    public String getFormattedValueByExternalKey(String externalKey) {
        return formattedValuesMapList.get(ItemAPIDAOImpl.attributeExternalKey.get(itemType).get(externalKey));
    }

    /**
     * Returns custom Attribute's Value of the item by attribute ExternalKey.
     *
     * @param externalKey : string external key field.
     * @return String: custom Attribute's Value of the Item.
     */
    public String getValueByExternalKey(String externalKey) {
        return valuesMapList.get(ItemAPIDAOImpl.attributeExternalKey.get(itemType).get(externalKey));
    }

    /**
     * @return The item type of the current item
     */
    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    /**
     * Returns a list of State IDs of a Workflow
     *
     * @return List: list of State IDs
     */
    public List<Long> getStateIds() {
        return stateIds;
    }

    public void setStateIds(List<Long> stateIds) {
        this.stateIds = stateIds;
    }
}
