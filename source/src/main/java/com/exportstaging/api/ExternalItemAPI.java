package com.exportstaging.api;

import com.exportstaging.api.domain.Attribute;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.implementation.ItemApiImpl;
import com.exportstaging.api.resultset.ItemsResultSet;
import com.exportstaging.api.searchfilter.SearchFilterCriteria;
import com.exportstaging.api.wraper.ItemWrapper;
import com.exportstaging.api.wraper.RecordWrapper;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.annotation.PreDestroy;
import java.util.*;

/**
 * The ExternalItemAPI class is reads the Record, Item as well as Attribute data from export database using its standard APIs.
 * This class implements the ItemAPIs and RecordAPIs wraper.
 * When creating an object must pass the itemType i.e Pdmarticle/Mamfile <i>ExternalItemAPI(String itemType)</i>
 * Following are the Item types used to access data for specific<br>
 * <b>ITEM_TYPE_PRODUCT  : </b> For Pdmarticle<br>
 * <b>ITEM_TYPE_VIEW     : </b> For Pdmarticlestructure<br>
 * <b>ITEM_TYPE_FILE     : </b> For Mamfile<br>
 * <b>ITEM_TYPE_USER     : </b> For User<br>
 * <b>ITEM_TYPE_WORKFLOW : </b> For Workflow<br>
 */
public class ExternalItemAPI implements ItemAPIs, RecordAPIs {

    private ConfigurableApplicationContext context = null;
    private ItemApiImpl itemApiImpl = null;
    protected String itemType = null;


    @Deprecated
    public ExternalItemAPI() {
        //TODO remove this default constructor.
    }

    /**
     * Constructor of ExportItemAPI for setting the specific item type for accessing export database data vai APIs.
     *
     * @param itemType Item type i.e Pdmarticle/Mamfile
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */
    public ExternalItemAPI(String itemType) throws ExportStagingException {
        try {
            initialiseResources();
            if (checkItemType(itemType)) {
                this.itemType = itemType;
                itemApiImpl.init(itemType);
            } else {
                String msg = "Invalid item type: " + itemType + " not supported";
                System.out.println(msg);
                throw new ExportStagingException(msg);
            }
        } catch (ExportStagingException e) {
            this.close();
            throw new ExportStagingException(e);
        }
    }

    /**
     * Returns the item type for which @{@link ExternalItemAPI} object is created.
     *
     * @return Item type
     */
    public String getItemType() {
        return itemType;
    }


    private void initialiseResources() {
        try {
            if (context == null || !context.isActive()) {
                context = new ClassPathXmlApplicationContext("spring/api.xml");
                getItemApiImplObject();
            }
        } catch (Exception e) {
            System.out.println("Make sure properties class path is properly configured in configuration.xml file");
            throw e;
        }
    }

    private  void getItemApiImplObject() {
        if (itemApiImpl == null) {
            itemApiImpl = (ItemApiImpl) context.getBean("itemApiImpl");
        }
    }

    private ItemsResultSet getItemsResultSet() {
        try {
            return (ItemsResultSet) context.getBean("itemsResultSet");
        } catch (Exception e) {
            context.refresh();
            return (ItemsResultSet) context.getBean("itemsResultSet");
        }
    }

    private boolean checkItemType(String itemType) throws ExportStagingException {
        return itemApiImpl.checkItemType(itemType);
    }

    private boolean ItemTypeConsistency(String itemType) {
        return itemType.equals(getItemType());
    }

    private ItemWrapper getItemById(String itemType, long itemID) throws ExportStagingException {
        initialiseResources();
        if (ItemTypeConsistency(itemType))
            return itemApiImpl.getItemById(itemType, itemID);
        else
            throw new ExportStagingException("The ExternalItemAPI object has ItemType: " + this.itemType);
    }


    private RecordWrapper getItemByStateID(String itemType, long stateID) throws ExportStagingException {
        initialiseResources();
        if (ItemTypeConsistency(itemType))
            return itemApiImpl.getItemByStateID(itemType, stateID);
        else
            throw new ExportStagingException("The ExternalItemAPI object has ItemType: " + this.itemType);
    }


    @Override
    public ItemWrapper getItemById(long itemID) throws ExportStagingException, NullPointerException {
        return getItemById(this.itemType, itemID);
    }

    @Override
    public RecordWrapper getItemByStateID(long stateID) throws ExportStagingException {
        return getItemByStateID(this.itemType, stateID);
    }

    @Override
    public List<String> getAllFields() throws ExportStagingException {
        return itemApiImpl.getAllFields(itemType);
    }

    @Override
    public List<String> getStandardFields() throws ExportStagingException {
        return itemApiImpl.getStandardFields(itemType);
    }

    @Override
    public List<Long> getCustomFields() throws ExportStagingException {
        return itemApiImpl.getCustomFields(itemType);
    }

    @Override
    public List<Long> getPluginFields() throws ExportStagingException {
        return itemApiImpl.getPluginFields(itemType);
    }

    private ItemWrapper getItemByExternalKey(String itemType, String itemExternalKey) throws ExportStagingException {
        initialiseResources();
        if (ItemTypeConsistency(itemType))
            return itemApiImpl.getItemByExternalKey(itemType, itemExternalKey);
        else
            throw new ExportStagingException("The ExternalItemAPI object has ItemType: " + this.itemType);
    }

    @Override
    public ItemWrapper getItemByExternalKey(String itemExternalKey) throws ExportStagingException, NullPointerException {
        return getItemByExternalKey(itemType, itemExternalKey);
    }

    private Set<Long> getItemIdsByFilterFromMaterializeView(String itemType, SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException {
        initialiseResources();
        return itemApiImpl.getItemIdsByFilterFromMaterializeView(itemType, searchFilterCriteria);
    }

    private Set<Long> getItemIdsByFilters(String itemType, SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException {
        initialiseResources();
        if (ItemTypeConsistency(itemType))
            return itemApiImpl.getItemIdsByFilters(itemType, searchFilterCriteria);
        else
            throw new ExportStagingException("The ExternalItemAPI object has ItemType: " + this.itemType);
    }

    @Override
    public Set<Long> getItemIdsByFilters(SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException, NullPointerException {
        return getItemIdsByFilters(itemType, searchFilterCriteria);
    }

    @Override
    public Set<Long> getUpdatedItemIDs(String date, SearchFilterCriteria criteria) throws ExportStagingException {
        return itemApiImpl.getUpdatedItemIDs(date, criteria);
    }

    @Override
    public Set<Long> getCreatedItemIDs(String date, SearchFilterCriteria criteria) throws ExportStagingException {
        return itemApiImpl.getCreatedItemIDs(date, criteria);
    }

    @Override
    public Set<Long> getUpdatedItemIDs(String date) throws ExportStagingException {
        return itemApiImpl.getUpdatedItemIDs(date);
    }

    @Override
    public Set<Long> getCreatedItemIDs(String date) throws ExportStagingException {
        return itemApiImpl.getCreatedItemIDs(date);
    }

    private Attribute getAttributeByID(String itemType, long attributeID) throws ExportStagingException {
        initialiseResources();
        if (ItemTypeConsistency(itemType))
            return itemApiImpl.getAttributeByID(itemType, attributeID);
        else
            throw new ExportStagingException("ItemType Mismatch");
    }

    @Override
    public Attribute getAttributeByID(long attributeID) throws ExportStagingException, NullPointerException {
        return getAttributeByID(itemType, attributeID);
    }

    private Attribute getAttributeByExternalKey(String itemType, String attributeExternalkey)
            throws ExportStagingException {
        initialiseResources();
        if (ItemTypeConsistency(itemType))
            return itemApiImpl.getAttributeByExternalKey(itemType, attributeExternalkey);
        else
            throw new ExportStagingException("ItemType Mismatch");
    }

    @Override
    public Attribute getAttributeByExternalKey(String attributeExternalKey)
            throws ExportStagingException, NullPointerException {
        return getAttributeByExternalKey(itemType, attributeExternalKey);
    }

    private List<Long> getAttributeIDsByClassID(long classID, String itemType) throws ExportStagingException {
        initialiseResources();
        if (ItemTypeConsistency(itemType))
            return itemApiImpl.getAttributeIDsByClassID(classID, itemType);
        else
            throw new ExportStagingException("ItemType Mismatch");
    }

    @Override
    public List<Long> getAttributeIDsByClassID(long classID) throws ExportStagingException {
        return getAttributeIDsByClassID(classID, itemType);
    }

    private ItemsResultSet getItemsByFilter(String itemType, SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException {
        initialiseResources();
        if (ItemTypeConsistency(itemType))
            return getItemResultSet(itemType, searchFilterCriteria);
        else
            throw new ExportStagingException("ItemType Mismatch");
    }

    @Override
    public ItemsResultSet getItemsByFilter(SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException, NullPointerException {
        return getItemsByFilter(itemType, searchFilterCriteria);
    }

    /**
     * MaterializedView are created only for few standard fields.
     * The filter will be applied on that and get the list of resultant IDs
     *
     * @param searchFilterCriteria filter criteria that should be executed on MaterializedView.
     * @return list of result of IDs.
     * @throws ExportStagingException in case of exception exportstaging exception will be thrown.
     */
    public ItemsResultSet getItemIdByFilterFromMaterializeView(SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException {
        if (ItemTypeConsistency(itemType)) {
            Set<Long> itemIDs = getItemIdsByFilterFromMaterializeView(itemType, searchFilterCriteria);
            return prepareResultSet(itemIDs, searchFilterCriteria);
        } else
            throw new ExportStagingException("ItemType Mismatch");
    }

    private ItemsResultSet prepareResultSet(Set<Long> itemIDs, SearchFilterCriteria searchFilterCriteria) {
        ItemsResultSet itemsResultSet = getItemsResultSet();
        itemsResultSet.setItemType(itemType);
        List<Long> itemIDList = new ArrayList<>();
        itemIDList.addAll(itemIDs);
        itemsResultSet.setItemIdList(itemIDList);

        if (searchFilterCriteria.getLanguage()) {
            itemsResultSet.setLanguageShortName(searchFilterCriteria.getLanguageShortNameValue());
        }
        return itemsResultSet;
    }

    private ItemsResultSet getItemResultSet(String itemType, SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException {
        if (ItemTypeConsistency(itemType)) {
            Set<Long> itemIDs = getItemIdsByFilters(itemType, searchFilterCriteria);
            ItemsResultSet itemsResultSet = getItemsResultSet();
            itemsResultSet.setItemType(itemType);
            List<Long> itemIDList = new ArrayList<>();
            itemIDList.addAll(itemIDs);
            itemsResultSet.setItemIdList(itemIDList);

            if (searchFilterCriteria.getLanguage()) {
                itemsResultSet.setLanguageShortName(searchFilterCriteria.getLanguageShortNameValue());
            }
            return itemsResultSet;
        } else
            throw new ExportStagingException("ItemType Mismatch");
    }

    @Override
    public List<String> getLanguagesShortName() throws ExportStagingException {
        return itemApiImpl.getLanguagesShortName(itemType);
    }

    public String getDefaultLanguage() {
        return itemApiImpl.getDefaultShortName();
    }

    public long getDefaultLanguageID() {
        return itemApiImpl.getDefaultLanguageID();
    }

    private List<Attribute> getAttribute(long ID, int depth, String itemType)
            throws ExportStagingException {
        initialiseResources();
        if (ItemTypeConsistency(itemType))
            return itemApiImpl.getAttribute(ID, depth, itemType);
        else
            throw new ExportStagingException("ItemType Mismatch");
    }

    @Override
    public List<Attribute> getAttribute(long IDorName, int depth) throws ExportStagingException {
        return getAttribute(IDorName, depth, itemType);
    }

    @Override
    public Set<Integer> getLanguageIDs() throws ExportStagingException {
        initialiseResources();
        if (!checkItemType(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE)) {
            throw new ExportStagingException("Language table not configured");
        }
        return itemApiImpl.getLanguageIDs();
    }

    @Override
    public Map<Integer, String> getLanguageShortNames() throws ExportStagingException {
        initialiseResources();
        if (!checkItemType(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE)) {
            throw new ExportStagingException("Language table not configured");
        }
        return itemApiImpl.getLanguageShortNames();
    }

    @Override
    public Map<String, String> getLanguageByID(int languageID) throws ExportStagingException {
        initialiseResources();
        if (!checkItemType(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE)) {
            throw new ExportStagingException("Language table not configured");
        }
        return itemApiImpl.getLanguageData(languageID);
    }

    @Override
    public Map<String, String> getLanguageByShortName(String languageShortName) throws ExportStagingException {
        Map<Integer, String> languageShortNames = getLanguageShortNames();
        for (int languageID : languageShortNames.keySet()) {
            if (languageShortNames.get(languageID).equals(languageShortName.trim())) {
                return getLanguageByID(languageID);
            }
        }
        return new HashMap<>();
    }

    @Override


    @PreDestroy
    protected void finalize() {
        if (context != null) {
            context.close();
        }
    }

    public void close() {
        if (context != null) {
            context.close();
        }
        itemApiImpl = null;
    }
}