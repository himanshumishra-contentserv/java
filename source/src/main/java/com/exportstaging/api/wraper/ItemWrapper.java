package com.exportstaging.api.wraper;

import com.exportstaging.api.domain.Item;
import com.exportstaging.api.domain.Reference;
import com.exportstaging.api.domain.Subtable;
import com.exportstaging.api.exception.ExportStagingException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The ItemWrapper interface specifies the Item and Attribute specific apis.
 * This interface extends the RecordWrapper.
 * Following are the Item types used to access data for specific<br>
 * <b>ITEM_TYPE_PRODUCT  : </b> For Pdmarticle<br>
 * <b>ITEM_TYPE_VIEW     : </b> For Pdmarticlestructure<br>
 * <b>ITEM_TYPE_FILE     : </b> For Mamfile<br>
 * <b>ITEM_TYPE_USER     : </b> For User<br>
 */
public interface ItemWrapper extends RecordWrapper {
    /**
     * @return the default languageID set inside ContentServ
     */
    int getDefaultLanguageID();


    void setDefaultLanguageID(int defaultLanguageID);


    /**
     * @return ID of the Item which is parent to this Item
     */
    long getParentItemID();

    /**
     * @return IsFolder value of the current Item
     */
    int getIsFolder();

    /**
     * @return List of Classes assigned to the Item
     */
    List<Long> getClassMapping();

    /**
     * @return ID of the default language
     */
    long getLanguageID();

    /**
     * Returns the ID of the provided language short name
     *
     * @param languageShortName Language Short name for the expected ID
     * @return ID of the provided language short name
     */
    long getLanguageID(String languageShortName);

    /**
     * Provides the formatted-value (UI Visible value) for all simple attributes of the current item in specified
     * language
     *
     * @param languageShortName ShortName of the Language in which data is expected
     * @return Map of formattedValue against attribute ID
     */
    Map<Long, String> getFormattedValuesMapList(String languageShortName);

    /**
     * Fetches the IDs of the children Items to the specified depth
     *
     * @param level Depth of the child levels to fetch
     * @return Set of IDs of all children of the current item (upto the provided level)
     * @throws ExportStagingException {@link ExportStagingException}
     */
    Set<Long> getItemIds(int level) throws ExportStagingException;

    /**
     * @return returns the list of all attributes assigned to the current item
     */
    List<Long> getAttributeIDs();

    /**
     * @return Returns the {@link Item} object for the immediate parent in default language
     * @throws ExportStagingException {@link ExportStagingException}
     */
    Item getParent() throws ExportStagingException;

    /**
     * Fetch the formatted value (UI Visible value) of the attribute in specified language
     *
     * @param attributeId       ID of the attribute
     * @param languageShortName Language short name
     * @return Formatted Value Data for the specified ID in requested language
     */
    String getFormattedValueByID(long attributeId, String languageShortName);

    /**
     * Fetch the value (Database value) of the attribute in specified language
     *
     * @param attributeId       ID of the attribute
     * @param languageShortName Language short name
     * @return Value for the specified attributeID in requested language
     */
    String getValueByID(long attributeId, String languageShortName);

    /**
     * Fetch the formatted value (UI Visible value) of the attribute in default language
     *
     * @param attributeId ID of the attribute
     * @return Formatted Value for the specified attributeID in default language
     */
    String getFormattedValueByID(long attributeId);

    /**
     * Fetch the value (Database value) of the attribute in default language
     *
     * @param attributeId ID of the attribute
     * @return Value for the specified attributeID in default language
     */
    String getValueByID(long attributeId);


    /**
     * Fetch the formatted value (UI Visible value) of the attribute in default language
     *
     * @param externalKey Attribute ExternalKey
     * @return Formatted Value for the specified attribute externalkey in default language
     */
    String getFormattedValueByExternalKey(String externalKey);


    /**
     * Fetch the formatted value (UI Visible value) of the attribute in specified language
     *
     * @param externalKey       Attribute ExternalKey
     * @param languageShortName Language short name
     * @return Formatted Value Data for the specified externalKey in requested language
     */
    String getFormattedValueByExternalKey(String externalKey, String languageShortName);


    /**
     * Fetch the value (Database value) of the attribute in default language
     *
     * @param externalKey Attribute ExternalKey
     * @return Value for the specified attribute externalKey in default language
     */
    String getValueByExternalKey(String externalKey);

    /**
     * Fetch the value (Database value) of the attribute in specified language
     *
     * @param externalKey       Attribute ExternalKey
     * @param languageShortName Language short name
     * @return Value for the specified attribute externalKey in requested language
     */
    String getValueByExternalKey(String externalKey, String languageShortName);

    /**
     * Fetch the List of {@link Reference} attached to this item in default language
     *
     * @param attributeId Reference Attribute ID
     * @return All the referenced products assigned to this item
     */
    List<Reference> getReferencesByID(long attributeId);

    /**
     * Fetch the List of {@link Reference} attached to this item in specified language
     *
     * @param attributeId       Reference Attribute ID
     * @param languageShortName Language ShortName in which data is to be fetched
     * @return All the referenced products assigned to this item in the specified language
     */
    List<Reference> getReferencesByID(long attributeId, String languageShortName);

    /**
     * Fetch the List of {@link Subtable} attached to this item in default language
     *
     * @param attributeID Subtable Attribute ID
     * @return All the subtable present for the specified attribute in default language
     */
    @Deprecated
    List<Subtable> getTableByID(long attributeID);

    /**
     * Fetch the List of {@link Subtable} attached to this item in specified language
     *
     * @param attributeId       Subtable Attribute ID
     * @param languageShortName Language ShortName in which data is to be fetched
     * @return All the subtable present for the specified attribute in specified language
     */
    @Deprecated
    List<Subtable> getTableByID(long attributeId, String languageShortName);

    /**
     * Fetch the List of {@link Subtable} rows of an item in default language
     *
     * @param attributeID Subtable Attribute ID
     * @return Rows of subtable present for the specified attribute in default language
     */
    List<Subtable> getTableRowsByAttributeID(long attributeID);

    /**
     * Fetch the List of {@link Subtable} rows of an item in specified language
     *
     * @param attributeId       Subtable Attribute ID
     * @param languageShortName Language ShortName in which data is to be fetched
     * @return Rows of subtable present for the specified attribute in specified language
     */
    List<Subtable> getTableRowsByAttributeID(long attributeId, String languageShortName);

    /**
     * Fetch the List of {@link Subtable} attached to this item in default language
     *
     * @param externalKey Attribute External Key
     * @return All the subtable present for the specified attribute in default language
     */
    @Deprecated
    List<Subtable> getTableByExternalKey(String externalKey);

    /**
     * Fetch the List of {@link Subtable} attached to this item in specified language
     *
     * @param externalKey       Attribute External Key
     * @param languageShortName Language ShortName in which data is to be fetched
     * @return All the subtable present for the specified attribute in specified language
     */
    @Deprecated
    List<Subtable> getTableByExternalKey(String externalKey, String languageShortName);

    /**
     * Fetch the List of {@link Subtable} rows data of an item in default language
     *
     * @param attributeExternalKey Attribute External Key
     * @return Rows of subtable present for the specified attributeExternalKey in default language
     */
    List<Subtable> getTableRowsByExternalKey(String attributeExternalKey);

    /**
     * Fetch the List of {@link Subtable} rows data of an item in specified language
     *
     * @param attributeExternalKey Attribute External Key
     * @param languageShortName    Language ShortName in which data is to be fetched
     * @return Rows of subtable present for the specified attributeExternalKey in specified language
     */
    List<Subtable> getTableRowsByExternalKey(String attributeExternalKey, String languageShortName);

    /**
     * Fetch the List of {@link Reference} attached to this item in default language
     *
     * @param externalKey Reference Attribute ExternalKey
     * @return All the referenced products assigned to this item in the default language
     */
    List<Reference> getReferencesByExternalKey(String externalKey);

    /**
     * Fetch the List of {@link Reference} attached to this item in specified language
     *
     * @param externalKey       Reference Attribute External
     * @param languageShortName Language ShortName in which data is to be fetched
     * @return All the referenced products assigned to this item in the specified language
     */
    List<Reference> getReferencesByExternalKey(String externalKey, String languageShortName);

    /**
     * Fetches the FormattedValue (UI Visible value) for the current item
     *
     * @param field AttributeID
     * @return FormattedValue for the attribute in default language
     */
    String getFormattedValue(String field);

    /**
     * Fetches the FormattedValue (UI Visible value) for the current item in specified language
     *
     * @param field             AttributeID
     * @param languageShortName Language ShortName for the language to fetch data
     * @return FormattedValue for the attribute in default language
     */
    String getFormattedValue(String field, String languageShortName);
}
