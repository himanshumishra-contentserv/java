package com.exportstaging.api.implementation;

import com.exportstaging.api.dao.ItemAPIDAOImpl;
import com.exportstaging.api.domain.*;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.wraper.ItemWrapper;
import com.exportstaging.api.wraper.RecordWrapper;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Serialize plain old Java object (POJO) of Item which contain all information
 * related to a CONTENTSERV Item in all the languages.
 */

@Scope("prototype")
@Component("wrapperImpl")
public class WrapperImpl implements ItemWrapper, RecordWrapper {

    private HashMap<String, Item> languagesItem;
    private HashMap<Long, String> languageIdsShortNamesMapping;
    private String defaultLanguageShortName;
    private int defaultLanguageID;
    private String itemType;
    @Autowired
    private ItemAPIDAOImpl itemAPIDAOImpl;

    @Override
    public int getDefaultLanguageID() {
        return defaultLanguageID;
    }

    @Override
    public void setDefaultLanguageID(int defaultLanguageID) {
        this.defaultLanguageID = defaultLanguageID;
    }

    @Override
    public String getItemType() {
        return itemType;
    }

    @Override
    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    @Override
    public String getDefaultLanguageShortName() {
        return defaultLanguageShortName;
    }

    @Override
    public void setDefaultLanguageShortName(String defaultLanguageShortName) {
        this.defaultLanguageShortName = defaultLanguageShortName;
    }

    @Override
    public HashMap<String, Item> getLanguagesItem() {
        return languagesItem;
    }

    @Override
    public void setLanguagesItem(HashMap<String, Item> languagesItem) {
        this.languagesItem = languagesItem;
    }

    /**
     * Returns ID of Item.(ContentServ Item ID)
     *
     * @return String: ContentServ ID of the Item.
     */

    @Override
    @JsonIgnore
    public long getItemID() {
        return this.getLanguagesItem().get(defaultLanguageShortName).getItemID();
    }

    /**
     * Returns ParentID of the Item.(get the CONTENTSERV ID of the parant item.)
     *
     * @return String: ParentID of the Item.
     */

    @Override
    @JsonIgnore
    public long getParentItemID() {
        return this.getLanguagesItem().get(defaultLanguageShortName).getParentID();
    }

    /**
     * Returns IsFolder of the item.(return the information that current Item is
     * folder or not)
     *
     * @return String: IsFolder of the Item.
     */

    @Override
    @JsonIgnore
    public int getIsFolder() {
        return this.getLanguagesItem().get(defaultLanguageShortName).getIsFolder();
    }

    /**
     * Returns ClassMapping of the item which contain the attributes that are related
     * to the current Item.
     *
     * @return List long: ClassMapping of the Item.
     */

    @Override
    @JsonIgnore
    public List<Long> getClassMapping() {
        return this.getLanguagesItem().get(defaultLanguageShortName).getClassMapping();
    }

    /**
     * Returns External Key of Item which is use for manage Item from outside of
     * CONTENTSERV.
     *
     * @return String: External Key of the Item.
     */

    @Override
    @JsonIgnore
    public String getExternalKey() {
        return this.getLanguagesItem().get(defaultLanguageShortName).getExternalKey();
    }

    /**
     * Returns the LanguageID of the current Item.(the current Item data language
     * eg. 1 for english, 2 for german )
     *
     * @return String: LanguageID of the Item.
     */

    @Override
    @JsonIgnore
    public long getLanguageID() {
        return this.getLanguagesItem().get(defaultLanguageShortName).getLanguageID();
    }

    /**
     * Returns the LanguageID of the current Item.(the current Item data language
     * eg. 1 for english, 2 for german )
     *
     * @param languageShortName : String Containing the Language Short Name for the required
     *                          language
     * @return String: LanguageID of the Item.
     */

    @Override
    @JsonIgnore
    public long getLanguageID(String languageShortName) {
        return this.getLanguagesItem().get(languageShortName).getLanguageID();
    }

    /**
     * Returns the Language short Name of the current Item.(the current Language
     * short Name for Item data)
     *
     * @return String: Language short Name of the Item.
     */

    @Override
    @JsonIgnore
    public String getLanguageShortName() {
        return defaultLanguageShortName;
    }

    /**
     * Returns the Language short Name in required language.
     *
     * @param languageID : String Containing the Language Short Name for the required
     *                   language
     * @return String: Language short Name of the Item.
     */

    @Override
    @JsonIgnore
    public String getLanguageShortName(String languageID) {
        return this.getLanguageIdsShortNamesMapping().get(Long.parseLong(languageID));
    }

    @Override
    @JsonIgnore
    public Map<Long, String> getFormattedValuesMapList(String languageShortName) {
        return this.getLanguagesItem().get(languageShortName).getFormattedValuesMapList();
    }

    @JsonIgnore
    private Map<String, String> getStandardValuesMapList(String languageShortName) {
        return this.getLanguagesItem().get(languageShortName).getStandardValuesMapList();
    }

    @JsonIgnore
    private Map<Long, String> getValuesMapList(String languageShortName) {
        return this.getLanguagesItem().get(languageShortName).getValuesMapList();
    }

    @JsonIgnore
    private List<Reference> getReferenceValueList(String languageShortName) {
        return this.getLanguagesItem().get(languageShortName).getReferenceValueList();
    }

    @JsonIgnore
    private List<Subtable> getSubTableValueList(String languageShortName) {
        return this.getLanguagesItem().get(languageShortName).getSubTableValueList();
    }

    /**
     * Returns all immediate children Item IDs of the item.
     *
     * @param level : integer level.
     * @return String: children item IDs of the Item.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom Exception
     */

    @Override
    @JsonIgnore
    public Set<Long> getItemIds(int level) throws ExportStagingException {
        return this.getLanguagesItem().get(defaultLanguageShortName).getItemIds(level, itemType);
    }

    /**
     * Returns all custom Attribute Ids of the item.
     *
     * @return long: custom Attribute Ids of the Item.
     */

    @Override
    @JsonIgnore
    public List<Long> getAttributeIDs() {
        return this.getLanguagesItem().get(defaultLanguageShortName).getAttributeIDs();
    }

    /**
     * Returns Item object for the current Item's parent.
     *
     * @return String: Item object for the current Item's parent
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom Exception
     */
    @Override
    @JsonIgnore
    public Item getParent() throws ExportStagingException {
        return itemAPIDAOImpl.getParentData(getParentItemID(), itemType).getLanguagesItem().get(defaultLanguageShortName);
    }

    /**
     * Returns custom Attribute's Formatted Value of the item by attribute ID.
     *
     * @param attributeId       : long Attribute ID.
     * @param languageShortName : string short name of language.
     * @return String: custom Attribute's Formatted Value of the Item.
     */

    @Override
    @JsonIgnore
    public String getFormattedValueByID(long attributeId, String languageShortName) {
        return getFormattedValuesMapList(languageShortName).get(attributeId);
    }

    /**
     * Returns custom Attribute's Value of the item by attribute ID.
     *
     * @param attributeId       : long Attribute ID.
     * @param languageShortName : string short name of language.
     * @return String: custom Attribute's Value of the Item.
     */

    @Override
    @JsonIgnore
    public String getValueByID(long attributeId, String languageShortName) {
        return getValuesMapList(languageShortName).get(attributeId);
    }

    /**
     * Returns custom Attribute's Formatted Value of the item by attribute ID.
     *
     * @param attributeId : long attribute id.
     * @return String: custom Attribute's Formatted Value of the Item.
     */

    @Override
    @JsonIgnore
    public String getFormattedValueByID(long attributeId) {
        return getFormattedValueByID(attributeId, defaultLanguageShortName);
    }

    /**
     * Returns custom Attribute's Value of the item by attribute ID.
     *
     * @param attributeId : long attribute id.
     * @return String: custom Attribute's Value of the Item.
     */

    @Override
    @JsonIgnore
    public String getValueByID(long attributeId) {
        return getValueByID(attributeId, defaultLanguageShortName);
    }

    /**
     * Returns custom Attribute's Value of the item by attribute ExternalKey.
     *
     * @param externalKey : string external key field.
     * @return String: custom Attribute's Value of the Item.
     */

    @Override
    @JsonIgnore
    public String getFormattedValueByExternalKey(String externalKey) {
        return getFormattedValueByExternalKey(externalKey, defaultLanguageShortName);
    }

    /**
     * Returns custom Attribute's Value of the item by attribute ExternalKey.
     *
     * @param externalKey       : string external key field.
     * @param languageShortName : string short name of language.
     * @return String: custom Attribute's Value of the Item.
     */

    @Override
    @JsonIgnore
    public String getFormattedValueByExternalKey(String externalKey, String languageShortName) {
        return getFormattedValuesMapList(languageShortName).
                get(ItemAPIDAOImpl.attributeExternalKey.get(itemAPIDAOImpl.getFormattedItemType(getItemType())).
                        get(externalKey));
    }

    @Override
    @JsonIgnore
    public String getValueByExternalKey(String externalKey) {
        return getValueByExternalKey(externalKey, defaultLanguageShortName);
    }

    @Override
    @JsonIgnore
    public String getValueByExternalKey(String externalKey, String languageShortName) {
        return getValuesMapList(languageShortName).
                get(ItemAPIDAOImpl.attributeExternalKey.get(itemAPIDAOImpl.getFormattedItemType(getItemType())).
                        get(externalKey));
    }

    /**
     * Returns References object of the reference attribute for the
     * Item.(reference to MAM, Reference to PIM)
     *
     * @param attributeId :long Attribute ID.
     * @return String     : References object of the reference attribute for the Item.
     */

    @Override
    @JsonIgnore
    public List<Reference> getReferencesByID(long attributeId) {
        return getReferencesByID(attributeId, defaultLanguageShortName);
    }

    /**
     * Returns References object of the reference attribute for the
     * Item.(reference to MAM, Reference to PIM)
     *
     * @param attributeId       :long Attribute ID.
     * @param languageShortName :string short name of language.
     * @return String: References object of the reference attribute for the Item.
     */

    @Override
    @JsonIgnore
    public List<Reference> getReferencesByID(long attributeId, String languageShortName) {
        List<Reference> referenceList = new ArrayList<>();
        List<Reference> referenceValueList = getReferenceValueList(languageShortName);
        for (Reference ref : referenceValueList) {
            if (ref.getAttributeID() == attributeId) {
                referenceList.add(ref);
            }
        }
        return referenceList;
    }

    /**
     * Returns subtable object of the subtable attribute for the Item.
     *
     * @param attributeID : ID of Attribute.
     * @return String     : subtable object of the subtable attribute for the Item.
     */

    @Override
    @JsonIgnore
    @Deprecated
    public List<Subtable> getTableByID(long attributeID) {
        return getTableRowsByAttributeID(attributeID, defaultLanguageShortName);
    }

    @Override
    @JsonIgnore
    @Deprecated
    public List<Subtable> getTableByID(long attributeId, String languageShortName) {
        return getTableRowsByAttributeID(attributeId, languageShortName);
    }

    @Override
    @JsonIgnore
    public List<Subtable> getTableRowsByAttributeID(long attributeID) {
        return getTableRowsByAttributeID(attributeID, defaultLanguageShortName);
    }

    @Override
    @JsonIgnore
    public List<Subtable> getTableRowsByAttributeID(long attributeId, String languageShortName) {
        List<Subtable> subtableList = new ArrayList<>();
        List<Subtable> subTableValueList = getSubTableValueList(languageShortName);
        for (Subtable sub : subTableValueList) {
            if (sub.getAttributeID() == attributeId) {
                subtableList.add(sub);
            }
        }
        return subtableList;
    }

    @Override
    @JsonIgnore
    @Deprecated
    public List<Subtable> getTableByExternalKey(String externalKey) {
        return getTableRowsByExternalKey(externalKey, defaultLanguageShortName);
    }

    @Override
    @JsonIgnore
    @Deprecated
    public List<Subtable> getTableByExternalKey(String externalKey, String languageShortName) {
        return getTableRowsByExternalKey(externalKey, languageShortName);
    }

    @Override
    public List<Subtable> getTableRowsByExternalKey(String attributeExternalKey) {
        return getTableRowsByExternalKey(attributeExternalKey, defaultLanguageShortName);
    }

    @Override
    public List<Subtable> getTableRowsByExternalKey(String attributeExternalKey, String languageShortName) {
        Long tableAttributeId = ItemAPIDAOImpl.attributeExternalKey.get(getItemType()).get(attributeExternalKey);
        if (tableAttributeId != null) {
            return getTableRowsByAttributeID(tableAttributeId, languageShortName);
        }
        return new ArrayList<>();
    }

    @Override
    @JsonIgnore
    public List<Reference> getReferencesByExternalKey(String externalKey) {
        return getReferencesByExternalKey(externalKey, defaultLanguageShortName);
    }

    @Override
    @JsonIgnore
    public List<Reference> getReferencesByExternalKey(String externalKey, String languageShortName) {
        Long referenceAttributeId = ItemAPIDAOImpl.attributeExternalKey.get(getItemType()).get(externalKey);
        if (referenceAttributeId != null) {
            return getReferencesByID(referenceAttributeId, languageShortName);
        }
        return new ArrayList<>();
    }

    @Override
    public ExportValues getValues() throws ExportStagingException {
        return getValues(defaultLanguageShortName);
    }

    @Override
    public ExportValues getValues(String languageShortName) throws ExportStagingException {
        List<AttributeValue> attributeValueList = new ArrayList<>();
        int languageID = ItemAPIDAOImpl.languageIDMap.get(languageShortName);
        Map<String, String> standardValueMap = getStandardValuesMapList(languageShortName);
        for (Map.Entry<String, String> entry : standardValueMap.entrySet()) {
            attributeValueList.add(new AttributeValue(entry.getKey(), languageID, entry.getValue(),
                    standardValueMap.get(entry.getKey())));
        }

        if (itemAPIDAOImpl.getItemTypes().contains(getItemType())) {
            Map<Long, String> formattedValueMap = getFormattedValuesMapList(languageShortName);
            Map<Long, String> valueMap = getValuesMapList(languageShortName);
            for (Map.Entry<Long, String> entry : formattedValueMap.entrySet()) {
                attributeValueList.add(new AttributeValue(String.valueOf(entry.getKey()), languageID, valueMap.get(entry.getKey()),
                        formattedValueMap.get(entry.getKey())));
            }
        }
        return new ExportValues(attributeValueList, getReferenceValueList(languageShortName),
                getSubTableValueList(languageShortName));
    }

    /**
     * Return the value of provided field.
     *
     * @param field :   string name of Field
     * @return : string value of provided field.
     */
    @Override
    public String getValue(String field) {
        return getValue(field, defaultLanguageShortName);
    }

    /**
     * Method will return all the standard fields of an Item.
     *
     * @return Set: Set of standard fields.
     */
    @Override
    public List<String> getStandardFields() {
        return new ArrayList<String>(this.getLanguagesItem().get(defaultLanguageShortName).getStandardValuesMapList().keySet());
    }

    @Override
    public String getValue(String field, String languageShortName) {
        if (field.substring(0, 1).matches("[0-9]") || field.substring(0, 1).matches("-")) {
            Map<Long, String> valueMap = this.getLanguagesItem().get(languageShortName).getValuesMapList();
            return valueMap.get(Long.parseLong(field));
        } else {
            Map<String, String> standardValuemap = this.getLanguagesItem().get(languageShortName).getStandardValuesMapList();
            return standardValuemap.get(field);
        }
    }

    @Override
    public List<Long> getStateIDs() {
        return this.getLanguagesItem().get(defaultLanguageShortName).getStateIds();
    }

    @Override
    public String getFormattedValue(String field) {
        return getFormattedValue(field, defaultLanguageShortName);
    }

    @Override
    public String getFormattedValue(String field, String languageShortName) {
        if (field.substring(0, 1).matches("[0-9]") || field.substring(0, 1).matches("-")) {
            Map<Long, String> formattedValueMap = this.getLanguagesItem().get(languageShortName).getFormattedValuesMapList();
            for (Map.Entry<Long, String> entry : formattedValueMap.entrySet()) {
                if (entry.getKey() == Long.parseLong(field)) {
                    return entry.getValue();
                }
            }
        } else {
            Map<String, String> standardValuemap = this.getLanguagesItem().get(languageShortName).getStandardValuesMapList();
            for (Map.Entry<String, String> entry : standardValuemap.entrySet()) {
                if (entry.getKey().equals(field)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    /**
     * Returns all language ids with it's language short name
     *
     * @return Key value pair of Language id and it's short name
     */
    public HashMap<Long, String> getLanguageIdsShortNamesMapping() {
        return languageIdsShortNamesMapping;
    }

    /**
     * Set all language ids with it's language short name
     *
     * @param languageIdsShortNamesMapping Map of key value pair of language ids and it's short name.
     */
    public void setLanguageIdsShortNamesMapping(HashMap<Long, String> languageIdsShortNamesMapping) {
        this.languageIdsShortNamesMapping = languageIdsShortNamesMapping;
    }
}
