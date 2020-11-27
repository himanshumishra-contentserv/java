package com.exportstaging.api.resultset;

import com.exportstaging.api.dao.ItemAPIDAOImpl;
import com.exportstaging.api.domain.Item;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.implementation.ItemApiImpl;
import com.exportstaging.api.implementation.WrapperImpl;
import com.exportstaging.api.wraper.ItemWrapper;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;

@Scope("prototype")
@Component("itemsResultSet")
public class ItemsResultSet implements ApplicationContextAware {

    @Value("${export.hashmap.batch.size}")
    private int batchSize;

    private int batchCount = 0;
    private String itemType;
    private List<Long> itemIdList = new ArrayList<>();
    private String languageShortName = null;
    private Map<Long, HashMap<Long, Item>> itemList = new HashMap<>();
    private boolean fetchData = true;
    private ApplicationContext context = null;

    @Autowired
    private ItemApiImpl itemApiImpl;
    @Autowired
    private ItemAPIDAOImpl itemAPIDAOImpl;

    public ItemsResultSet() {
    }

    private String prepareINOperatorDataQuery(String itemIDs) {
        return itemIDs.replace("[", "").replace("]", "");
    }

    private String getBatch() {
        String itemIDs = "";
        try {
            itemIDs = prepareINOperatorDataQuery(itemIdList.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return itemIDs;
    }

    public String getLanguageShortName() {
        return languageShortName;
    }

    public void setLanguageShortName(String languageShortName) {
        this.languageShortName = languageShortName;
    }

    public List<Long> getItemIdList() {
        return itemIdList;
    }

    public void setItemIdList(List<Long> itemIdList) {
        this.itemIdList = itemIdList;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    /**
     * Returns List of Item Object from the ItemsByFilter One by One.
     * Default Language will be used.
     *
     * @return :Item : ItemWrapper The List of Item Objects in different languages from
     * HashMap.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom Exception
     */
    public ItemWrapper nextItem() throws ExportStagingException {
        List<Long> defaultLanguageID = new ArrayList<>();
        defaultLanguageID.add(itemApiImpl.getDefaultLanguageID());
        return nextItem(defaultLanguageID);
    }

    /**
     * Returns List of Item Object from the ItemsByFilter One by One.
     *
     * @param LanguageID specify the language ID to get item object for specific langID
     * @return Object of {@link ItemWrapper} containing data in specified languages
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom Exception
     */
    public ItemWrapper nextItem(List<Long> LanguageID) throws ExportStagingException {
        ItemWrapper itemWrapper = context.getBean("wrapperImpl", WrapperImpl.class);
        itemWrapper.setDefaultLanguageShortName(itemAPIDAOImpl.getDefaultShortName());
        itemWrapper.setDefaultLanguageID((int) itemAPIDAOImpl.getDefaultLanguageID());
        itemWrapper.setItemType(itemType);

        List<Item> itemLanguageList = nextItemLanguage(itemType, LanguageID);
        HashMap<String, Item> itemDataList = new HashMap<>();
        HashMap<Long, String> languageIdAndShortName = new HashMap<>();
        if (!itemLanguageList.isEmpty()) {
            for (Item item : itemLanguageList) {
                String shortName = item.getLanguageShortName();
                itemDataList.put(shortName, item);
                languageIdAndShortName.put(item.getLanguageID(), shortName);
            }
            itemWrapper.setLanguagesItem(itemDataList);
            ((WrapperImpl) itemWrapper).setLanguageIdsShortNamesMapping(languageIdAndShortName);

            if (languageShortName != null) {
                itemWrapper.setDefaultLanguageShortName(languageShortName);
            }
            return itemWrapper;
        } else {
            return null;
        }
    }

    private String prepareLanguageFilter(List<Long> LanguageID) {
        return "IN (" + LanguageID.toString().replace("[", "").replace("]", "") + ")";
    }

    private List<Item> nextItemLanguage(String itemType, List<Long> LanguageID) throws ExportStagingException {
        List<Item> itemListTemp;
        String languageIdFilter = prepareLanguageFilter(LanguageID);
        if (fetchData) {
            String itemIds = getBatch();
            if (!itemIds.equals("")) {
                itemList = itemApiImpl.getItemByIdInBatch(itemIds, itemType, languageIdFilter);
                fetchData = false;
            }
        }
        itemListTemp = getItemData();
        return itemListTemp;
    }

    /**
     * Returns total count of Items from ItemsByFilter class according to the
     * Filter.
     *
     * @return : Integer The total count of Items.
     */
    public int count() {
        return itemIdList.size();
    }

    private List<Item> getItemData() {
        List<Item> itemListTemp = new ArrayList<>();
        for (Long itemId : itemList.keySet()) {
            HashMap<Long, Item> languageObjects = itemList.get(itemId);
            Set<Long> languageIds = languageObjects.keySet();
            for (long language : languageIds) {
                Item item = itemList.get(itemId).get(language);
                itemListTemp.add(item);
            }
            itemList.remove(itemId);
            break;
        }
        return itemListTemp;
    }

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        this.context = context;
    }
}