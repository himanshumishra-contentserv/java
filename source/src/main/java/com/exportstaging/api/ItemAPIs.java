package com.exportstaging.api;

import com.exportstaging.api.domain.Attribute;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.resultset.ItemsResultSet;
import com.exportstaging.api.searchfilter.SearchFilterCriteria;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface contains Item and Attributes specific APIs which is implement by ExternalItemAPI class to
 * export database data.
 * The methods in this interface can be used to access export database data
 * Following are the Item types used to access data for specific <br>
 * <b> ITEM_TYPE_PRODUCT : </b> For Pdmarticle<br>
 * <b> ITEM_TYPE_VIEW    : </b> For Pdmarticlestructure<br>
 * <b> ITEM_TYPE_FILE    : </b> For Mamfile<br>
 * <b> ITEM_TYPE_USER    : </b> For User<br>
 */

public interface ItemAPIs extends Apis {


    String ITEM_TYPE_PRODUCT = "Pdmarticle";
    String ITEM_TYPE_VIEW = "Pdmarticlestructure";
    String ITEM_TYPE_FILE = "Mamfile";
    String ITEM_TYPE_USER = "User";


    /**
     * MaterializedView are created only for few standard fields.
     * The filter will be applied on that and get the list of resultant IDs
     * Set only single field for filter
     *
     * @param searchFilterCriteria filter criteria that should be executed on MaterializedView.
     * @return list of result of IDs.
     * @throws ExportStagingException in case of exception exportstaging exception will be thrown.
     */
    public ItemsResultSet getItemIdByFilterFromMaterializeView(SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException;

    /**
     * Returns an Attribute Object containing all of the data according to the
     * Attribute ID and the Type.
     *
     * @param attributeID : The long which has the ID of Attribute.
     * @return Attribute: The Attribute Class Object which has all data for the
     * Attributes.
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */

    Attribute getAttributeByID(long attributeID) throws ExportStagingException;

    /**
     * Returns an Attribute Object containing all of the data according to the
     * Attribute External Key and the Type.
     *
     * @param attributeExternalKey : The String which has the External Key for the Attribute.
     * @return Attribute: The Attribute Class Object which has all data for the
     * Attributes.
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    Attribute getAttributeByExternalKey(String attributeExternalKey)
            throws ExportStagingException;

    /**
     * Returns The Attribute IDs which is in a specific class.
     *
     * @param classID : The long which has the classID.
     * @return attributeList : ArrayList The Attribute IDs which is in a specific class.
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    List<Long> getAttributeIDsByClassID(long classID) throws ExportStagingException;


    /**
     * Returns an Attribute Object containing all of the data according to the
     * Attribute ID/Name, language and the depth.
     *
     * @param IDorName The long which has the ID or Name of Attribute.
     * @param depth    The integer which has the depth of Attribute, By default is 0.
     * @return Attribute: The Attribute Class Object which has all data for the
     * Attributes.
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    List<Attribute> getAttribute(long IDorName, int depth)
            throws ExportStagingException;

    /**
     * Returns List of all Fields i.e. Custom Attributes and Standard Attributes
     *
     * @return List: List of all Fields i.e. Custom Attributes and Standard Attributes.
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    List<String> getAllFields() throws ExportStagingException;

    /**
     * Returns List of Standard Attributes
     *
     * @return List: List of Standard Attributes.
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    List<String> getStandardFields() throws ExportStagingException;

    /**
     * Returns List of Custom Attributes
     *
     * @return List: List of Custom Attributes.
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    List<Long> getCustomFields() throws ExportStagingException;

    /**
     * Returns List of Plugin Attributes
     *
     * @return List: List of Custom Attributes.
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    List<Long> getPluginFields() throws ExportStagingException;

    /**
     * Returns List of Language Short Name.
     *
     * @return List: Returns List of Language Short Name.
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    List<String> getLanguagesShortName() throws ExportStagingException;

    /**
     * Returns all existing language ids inside cassandra
     *
     * @return set of language ids
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    Set<Integer> getLanguageIDs() throws ExportStagingException;

    /**
     * Fetch all LanguageShortNames for all languages in cassandra
     *
     * @return A map of Language ID vs LanguageShortName
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    Map<Integer, String> getLanguageShortNames() throws ExportStagingException;

    /**
     * Should be called once per execution. It is important to call close to perform db instance clean-up and memory cleanup
     */
    void close();
}
