package com.exportstaging.api;

import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.wraper.RecordWrapper;

import java.util.Map;

/**
 * This interface contains specific APIs for Record which is implemented by ExternalItemAPI class to
 * export database data.
 * The methods in this interface can be used to access export database data
 * Following are the Record types used to access data for specific
 * <b> ITEM_TYPE_WORKFLOW : </b> For Workflow<br>
 */
public interface RecordAPIs extends Apis {

    String ITEM_TYPE_WORKFLOW = "Workflow";
    String ITEM_TYPE_LANGUAGE = "Language";

    /**
     * Returns {@link RecordWrapper} object containing Record data according to the Item Type and the state ID.
     *
     * @param stateID The long which has the workflow state ID.
     * @return {@link RecordWrapper} object. The WrapperImpl Object contains workflow data.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */
    RecordWrapper getItemByStateID(long stateID) throws ExportStagingException;

    /**
     * Fetch language information for requested languageid
     *
     * @param languageID int ID of the language
     * @return Language properties vs their values
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    Map<String, String> getLanguageByID(int languageID) throws ExportStagingException;

    /**
     * Fetch language information for requested languageShortName
     *
     * @param languageShortName String Language short name
     * @return Language properties vs their values
     * @throws ExportStagingException All exceptions will be wrapped in {@link ExportStagingException}
     */
    Map<String, String> getLanguageByShortName(String languageShortName) throws ExportStagingException;

    /**
     * Should be called once per execution. It is important to call close as it performs the closing of db instances
     * and clears the memory.
     */
    void close();
}
