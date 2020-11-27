package com.exportstaging.api;

import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.resultset.ItemsResultSet;
import com.exportstaging.api.searchfilter.SearchFilterCriteria;
import com.exportstaging.api.wraper.ItemWrapper;

import java.util.Set;

/**
 * This interface contains common APIs for both Record and Item which is extended by {@link RecordAPIs} and {@link ItemAPIs}
 * wrappers
 * The methods in this interface can be used to access export database data
 */

public interface Apis {

    /**
     * Returns {@link ItemWrapper} object containing Item data in all languages according
     * to the Item Type and the Item ID.
     *
     * @param itemID : The long which has the Record/Item ID.
     * @return :{@link ItemWrapper} object. The WrapperImpl Object contains Item object in
     * all languages.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */
    ItemWrapper getItemById(long itemID) throws ExportStagingException;

    /**
     * Returns WrapperImpl object containing Item data in all languages according
     * to the Item Type and the Item External Key.
     *
     * @param itemExternalKey : The String which has the External Key for the Item.
     * @return :WrapperImpl object. The WrapperImpl Object contains Item object in
     * all languages.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */
    ItemWrapper getItemByExternalKey(String itemExternalKey)
            throws ExportStagingException;

    /**
     * Returns List of Item IDs according to the Filter and the Item Type.
     *
     * @param searchFilterCriteria : The SearchFilterCriteria class object which has the Filters for
     *                             searching the Item IDs.
     * @return Set: String The Unique Set of Item IDs.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */
    Set<Long> getItemIdsByFilters(SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException;

    /**
     * Returns Object of ItemsByFilter class according to the Filter.
     *
     * @param searchFilterCriteria : The SearchFilterCriteria class object which has the Filters for
     *                             searching the Item.
     * @return :ItemsByFilter The Class which have some method for access the Item
     * List.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom Exception
     * @throws InterruptedException   If any type of Exception will throw Handle by this InterruptedException Exception
     */
    ItemsResultSet getItemsByFilter(SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException, InterruptedException;

    /**
     * Returns list of updated item ids.
     * The list contains items updated after the provided date.
     *
     * @param date get updated item ids from this date
     *             Format: yyyy-MM-dd, yyyy-MM-dd HH:mm:ss
     * @return list of updated item ids
     * @throws ExportStagingException Any type of exceptions will be enclosed inside {@link ExportStagingException}
     */
    Set<Long> getUpdatedItemIDs(String date) throws ExportStagingException;

    /**
     * Returns list of updated item ids.
     * The list contains items updated after the provided date and satisfying the given filter.
     *
     * @param date     get updated item ids from this date
     *                 Format: yyyy-MM-dd, yyyy-MM-dd HH:mm:ss
     * @param criteria {@link SearchFilterCriteria} object which holds filter conditions for data.
     * @return list of updated item ids
     * @throws ExportStagingException Any type of exceptions will be enclosed inside {@link ExportStagingException}
     */
    Set<Long> getUpdatedItemIDs(String date, SearchFilterCriteria criteria) throws ExportStagingException;

    /**
     * Returns list of created item ids.
     * The list contains items created after the provided date.
     *
     * @param date get created item ids from this date
     *             Format: yyyy-MM-dd, yyyy-MM-dd HH:mm:ss
     * @return list of created item ids
     * @throws ExportStagingException Any type of exceptions will be enclosed inside {@link ExportStagingException}
     */
    Set<Long> getCreatedItemIDs(String date) throws ExportStagingException;

    /**
     * Returns list of created item ids.
     * The list contains items created after the provided date and satisfying the given filter.
     *
     * @param date     get created item ids from this date
     *                 Format: yyyy-MM-dd, yyyy-MM-dd HH:mm:ss
     * @param criteria {@link SearchFilterCriteria} object which holds filter conditions for data.
     * @return list of created item ids
     * @throws ExportStagingException Any type of exceptions will be enclosed inside {@link ExportStagingException}
     */
    Set<Long> getCreatedItemIDs(String date, SearchFilterCriteria criteria) throws ExportStagingException;
}
