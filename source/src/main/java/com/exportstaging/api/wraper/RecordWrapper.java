package com.exportstaging.api.wraper;

import com.exportstaging.api.domain.ExportValues;
import com.exportstaging.api.domain.Item;
import com.exportstaging.api.exception.ExportStagingException;

import java.util.HashMap;
import java.util.List;

/**
 * The RecordWrapper interface specifies Record specific apis.
 * This interface extended by ItemWrapper.
 * Following are the Item types used to access data for specific<br><b>ITEM_TYPE_Workflow : </b> For Workflow<br>
 */
public interface RecordWrapper {

    /**
     * @return Returns the Item Type
     */
    String getItemType();

    /**
     * Sets the ItemType for the current Item Object
     *
     * @param itemType Valid Item Type
     */
    void setItemType(String itemType);

    /**
     * @return CONTENTSERV default language short name.
     */
    String getDefaultLanguageShortName();

    /**
     * Set the default language short name information in the current item object
     *
     * @param defaultLanguageShortName Default language short name
     */
    void setDefaultLanguageShortName(String defaultLanguageShortName);

    /**
     * @return Item object in all languages
     * Note: Structure : Map of languageShortName vs {@link Item} object.
     */
    HashMap<String, Item> getLanguagesItem();

    /**
     * Store the item object in all language
     *
     * @param languagesItem Map of LanguageShortName vs {@link Item} object
     */
    void setLanguagesItem(HashMap<String, Item> languagesItem);

    /**
     * @return Id of the Item stored inside this object
     */
    long getItemID();

    /**
     * @return ExternalKey of the Item stored inside this object
     */
    String getExternalKey();

    /**
     * @return LanguageShortName of the Item stored inside this object
     */
    String getLanguageShortName();

    /**
     * Provides language shortname for the provided languageID
     *
     * @param languageID LanguageID
     * @return Language Short Name corresponding to the provided languageID
     */
    String getLanguageShortName(String languageID);

    /**
     * @return Object of {@link ExportValues} which contains information about all attributes assigned to this item
     * in default language.
     * @throws ExportStagingException {@link ExportStagingException}
     */
    ExportValues getValues() throws ExportStagingException;

    /**
     * Object of {@link ExportValues} which contains information about all attributes assigned to this item
     * in specified language.
     *
     * @param languageShortName Language Short Name in which the data is requested
     * @return Attribute Data assigned to this Item
     * @throws ExportStagingException {@link ExportStagingException}
     */
    ExportValues getValues(String languageShortName) throws ExportStagingException;

    /**
     * Fetches the data for the requested simple attribute
     *
     * @param field ID of the attribute whose data is expected
     * @return value (Database value) of the requested attribute in default language
     */
    String getValue(String field);

    /**
     * @return List of all the standard fields available for the item
     */
    List<String> getStandardFields();

    /**
     * Fetches the data for the requested simple attribute in specified language short name
     *
     * @param field             ID of the attribute
     * @param languageShortName Language short name in which data is expected
     * @return Value (Database Value) of the requested attribute in specified language
     */
    String getValue(String field, String languageShortName);

    /**
     * @return the List of stateIDs
     */
    List<Long> getStateIDs();

    /**
     * Returns all language ids with it's language short name
     *
     * @return Key value pair of Language id and it's short name
     */
    public HashMap<Long, String> getLanguageIdsShortNamesMapping();
}
