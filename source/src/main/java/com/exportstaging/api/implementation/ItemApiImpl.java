package com.exportstaging.api.implementation;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.exportstaging.api.ExternalItemAPI;
import com.exportstaging.api.RecordAPIs;
import com.exportstaging.api.dao.ItemAPIDAOImpl;
import com.exportstaging.api.domain.*;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.searchfilter.SearchFilterCriteria;
import com.exportstaging.api.wraper.ItemWrapper;
import com.exportstaging.api.wraper.RecordWrapper;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.google.common.collect.Sets;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * ItemApiImpl implements the ItemAPIs interface.
 */
@Component("itemApiImpl")
public class ItemApiImpl extends ExternalItemAPI implements ApplicationContextAware {

    @Value("${cassandra.table.item.id}")
    private String sItemID;
    @Value("${cassandra.table.item.externalkey}")
    private String sExternalKey;
    @Value("${cassandra.suffix.view}")
    private String viewSuffix;
    @Value("${export.core.recordtypes}")
    private String recordTypes;
    @Value("${export.core.itemtypes}")
    private String itemTypes;
    private ApplicationContext context = null;

    @Autowired
    private ItemAPIDAOImpl itemAPIDAOImpl;
    private Map<String, List<Long>> specialConfigurations = new HashMap<>();
    private String languageShortName = null;
    private Map<String, Map<Long, List<Long>>> itemMappingData = new HashMap<>();
    private Map<String, List<Long>> customFields = new HashMap<>();
    private Map<String, List<String>> standardFields = new HashMap<>();
    private Map<String, List<Long>> pluginFields = new HashMap<>();
    private Map<String, List<String>> allFields = new HashMap<>();

    public void init(String itemType) throws ExportStagingException {
        super.itemType = itemType;
        itemAPIDAOImpl.init(itemType);
    }

    public ItemApiImpl() {
    }

    private List<Long> getSpecialConfigurations(String itemType) throws ExportStagingException {
        if (!specialConfigurations.containsKey(itemType)) {
            specialConfigurations.put(itemType, setSpecialConfigurations(itemType));
        }
        return specialConfigurations.get(itemType);
    }

    private List<Long> setSpecialConfigurations(String itemType) throws ExportStagingException {
        List<Long> specialConfigurations = new ArrayList<>();
        specialConfigurations.addAll(itemAPIDAOImpl.getAttributeFolderIDs(itemType));
        specialConfigurations.addAll(itemAPIDAOImpl.getReferenceConfigurationIDs(itemType));
        specialConfigurations.addAll(itemAPIDAOImpl.getSubtableConfigurationIDs(itemType));
        return specialConfigurations;
    }

    public List<String> getAllFields(String itemType) throws ExportStagingException {
        if (allFields.get(itemType) == null) {
            populateFieldList(itemType);
        }
        return allFields.get(itemType);
    }

    private void populateFieldList(String itemType) throws ExportStagingException {
        List<String> columnList = getColumns(itemType);
        List<String> allFieldList = new ArrayList<>();
        List<Long> customFieldList = new ArrayList<>();
        List<Long> pluginFieldList = new ArrayList<>();
        List<String> standardFieldList = new ArrayList<>();
        for (String element : columnList) {
            if (element.contains(":Value")) {
                String field = element.substring(0, element.indexOf(":"));
                //Note: if malformed column exists in Cassandra
                if (field.contains(",")) {
                    continue;
                }
                customFieldList.add(Long.parseLong(field));
                allFieldList.add(field);
                if (element.contains("-")) {
                    pluginFieldList.add(Long.parseLong(field));
                }
            } else if (element.contains(":FormattedValue")) {
                //Skip since it already populated from Value If Condition
            } else {
                standardFieldList.add(element);
                allFieldList.add(element);
            }
        }
        standardFields.put(itemType, standardFieldList);
        customFields.put(itemType, customFieldList);
        allFields.put(itemType, allFieldList);
        pluginFields.put(itemType, pluginFieldList);
    }

    public List<String> getStandardFields(String itemType) throws ExportStagingException {
        if (standardFields.get(itemType) == null) {
            populateFieldList(itemType);
        }
        return standardFields.get(itemType);
    }

    public List<Long> getPluginFields(String itemType) throws ExportStagingException {
        if (pluginFields.get(itemType) == null) {
            populateFieldList(itemType);
        }
        return pluginFields.get(itemType);
    }

    public List<Long> getCustomFields(String itemType) throws ExportStagingException {
        if (customFields.get(itemType) == null) {
            populateFieldList(itemType);
        }
        return customFields.get(itemType);
    }

    public boolean checkItemType(String itemType) throws ExportStagingException {
        return itemAPIDAOImpl.checkItemType(itemType);
    }

    public List<String> getLanguagesShortName(String itemType) throws ExportStagingException {
        return itemAPIDAOImpl.getLanguagesShortName(itemType);

    }

    public String getDefaultShortName() {
        return itemAPIDAOImpl.getDefaultShortName();
    }

    /**
     * Returns ItemWrapper object containing Item data in all languages according
     * to the Item Type and the Item ID.
     *
     * @param itemType : The String which has the Item Type. Default its NULL.
     * @param itemId   : The String which has the Item ID.
     * @return :ItemWrapper object. The ItemWrapper Object contains Item object in
     * all languages.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */

    public ItemWrapper getItemById(String itemType, long itemId) throws ExportStagingException {
        return itemAPIDAOImpl.prepareAndSetItemDataFromDB(itemType, sItemID, String.valueOf(itemId));
    }

    /**
     * Returns WrapperImpl object containing State data in default language according
     * to the Item Type and the Workflow state ID.
     *
     * @param itemType : The String which has the Item Type. Default its NULL.
     * @param stateId  : The Integer which has the State ID.
     * @return :WrapperImpl object. The WrapperImpl Object contains Item object in
     * all languages.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */

    public RecordWrapper getItemByStateID(String itemType, long stateId) throws ExportStagingException {
        return itemAPIDAOImpl.prepareAndSetItemDataFromDB(itemType, "StateID", String.valueOf(stateId));
    }

    /**
     * Returns Item Wrapper Object containing the Item data in all languages
     * according to the Item itemType and the Item External Key.
     *
     * @param itemType        : The String which has the Item Type. Default its NULL.
     * @param itemExternalKey : The String which has the External Key for the Item.
     * @return :ItemWrapper The List of Item Objects in different languages.
     * @throws ExportStagingException If any itemType of Exception will throw Handle by this Custom
     *                                Exception
     */
    public ItemWrapper getItemByExternalKey(String itemType, String itemExternalKey)
            throws ExportStagingException {
        return itemAPIDAOImpl.prepareAndSetItemDataFromDB(itemType, sExternalKey, itemExternalKey);
    }

    public Map<Long, HashMap<Long, Item>> getItemByIdInBatch(String itemIDs, String itemType, String languageIdFilter)
            throws ExportStagingException {
        return itemAPIDAOImpl.setItemTableData(itemIDs, itemType, languageIdFilter);
    }

    @Override
    public Set<Long> getUpdatedItemIDs(String date) throws ExportStagingException {
        return getItemIdsFromMVByField(date, "\"_LastWritten\" > '");
    }

    @Override
    public Set<Long> getUpdatedItemIDs(String date, SearchFilterCriteria criteria) throws ExportStagingException {
        Set<Long> itemIds = getUpdatedItemIDs(date);
        return getItemIdsByFilterAndIds(criteria, itemIds);
    }

    @Override
    public Set<Long> getCreatedItemIDs(String date) throws ExportStagingException {
        return getItemIdsFromMVByField(date, "\"_InsertTime\" > '");
    }

    @Override
    public Set<Long> getCreatedItemIDs(String date, SearchFilterCriteria criteria) throws ExportStagingException {
        Set<Long> itemIds = getCreatedItemIDs(date);
        return getItemIdsByFilterAndIds(criteria, itemIds);
    }

    private Set<Long> getItemIdsFromMVByField(String date, String field) throws ExportStagingException {
        SearchFilterCriteria criteria = new SearchFilterCriteria();
        criteria.setUserDefineFilter(field + date + "'");

        return getItemIdsByFilterFromMaterializeView(itemType, criteria);
    }

    private Set<Long> getItemIdsByFilterAndIds(SearchFilterCriteria criteria, Set<Long> itemIds) throws ExportStagingException {
        Set<Long> userItemIdsFilter = criteria.getItemIds();
        if (userItemIdsFilter != null) {
            itemIds = Sets.intersection(userItemIdsFilter, itemIds);
        }

        criteria.setItemIds(itemIds);
        return getItemIdsByFilters(criteria);
    }

    private List<Long> getStateIDs(long itemID, String itemType) throws ExportStagingException {
        return itemAPIDAOImpl.getWorkflowStateIDsfromDB(itemID, itemType);
    }

    private Item setRecordData(Row row, long itemID, String itemType) throws ExportStagingException {
        Item item = context.getBean("item", Item.class);

        item.setItemID(itemID);
        item.setItemType(itemType);
        item.setExternalKey(row.getString("ExternalKey"));
        if (recordTypes.contains(itemType)) {
            item.setLanguageShortName(itemAPIDAOImpl.getDefaultShortName());
        } else {
            item.setLanguageShortName(row.getString("LanguageShortName"));
        }
        List<String> standardFieldList = getStandardFields(itemType);
        Map<String, String> standardFieldValueMap = setStandardFieldValues(standardFieldList, row);

        item.setStandardValuesMapList(standardFieldValueMap);

        if (itemType.equalsIgnoreCase(ExternalItemAPI.ITEM_TYPE_WORKFLOW)) {
            item.setStateIds(getStateIDs(itemID, itemType));
        } else {
            List<Long> stateIdList = new ArrayList<>();
            String stateId = standardFieldValueMap.get(ExportMiscellaneousUtils.EXPORT_FIELD_STATEID);
            if (stateId != null)
                stateIdList.add(Long.parseLong(stateId));
            item.setStateIds(stateIdList);
        }
        return item;
    }

    public Item setItemData(Row row, long itemID, String itemType) throws ExportStagingException {
        Item item = null;
        List<Long> standardAttributeList = new ArrayList<>();
        try {
            item = this.setRecordData(row, itemID, itemType);

            if (itemTypes.contains(itemType)) {
                List<Long> classList = getClassListFromClassMapping(row.getString("ClassMapping"));
                long languageID = row.getLong("LanguageID");
                item.setIsFolder(row.getInt("IsFolder"));
                item.setClassMapping(classList);
                item.setLanguageID(languageID);
                item.setParentID(row.getLong("ParentID"));
                standardAttributeList.addAll(getAttributesIdList(classList, itemType));
                standardAttributeList.addAll(getPluginFields(itemType));
                if (itemType.contains(viewSuffix)) {
                    List<Long> extensionClassList = getClassListFromClassMapping(row.getString("_ExtensionClassMapping"));
                    if (!extensionClassList.isEmpty()) {
                        standardAttributeList.addAll(getAttributesIdList(extensionClassList, itemType));
                    }
                }

                //CSSUPINT-6698 fetching only assigned table type attribute data.
                List<Long> tableCSTypeAttributeIds = itemAPIDAOImpl.getSubtableConfigurationIDs(itemType);
                tableCSTypeAttributeIds.retainAll(standardAttributeList);

                List<Subtable> subtableList = itemAPIDAOImpl.getSubTableData(itemID, itemType, languageID, itemID, tableCSTypeAttributeIds);
                item.setSubTableValueList(subtableList);

                //CSSUPINT-6698 fetching only assigned reference type attribute data.
                List<Long> referenceCSTypeAttributeIds = itemAPIDAOImpl.getReferenceConfigurationIDs(itemType);
                referenceCSTypeAttributeIds.retainAll(standardAttributeList);

                List<Reference> referenceList = itemAPIDAOImpl.getReferenceData(String.valueOf(itemID), itemType, String.valueOf(languageID), String.valueOf(itemID), referenceCSTypeAttributeIds);
                item.setReferenceValueList(referenceList);

                List<Long> specialConfigurations = getSpecialConfigurations(itemType);
                standardAttributeList.removeAll(specialConfigurations);
                Map<String, Map<Long, String>> formattedAndValueMap = setFormattedAndValues(standardAttributeList, row);
                item.setFormattedValuesMapList(formattedAndValueMap.get("FormattedValue"));
                item.setValuesMapList(formattedAndValueMap.get("Value"));
                item.setAttributeIDs(standardAttributeList);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ExportStagingException(e.getMessage());
        }
        return item;
    }

    //TODO check for class mapping same for extensionClass Mapping
    private List<Long> getClassListFromClassMapping(String classMapping) {
        List<Long> dataList = new ArrayList<>();
        if (classMapping != null && !classMapping.equals("null") && !classMapping.equals("")) {
            for (String fieldData : classMapping.trim().split(" ")) {
                if (!fieldData.equals(""))
                    dataList.add(Long.valueOf(fieldData));
            }
        }
        return dataList;
    }

    private String getExternalKeyByAttributeId(long attributeId, String itemType) throws ExportStagingException {
        HashMap<String, String> attributeIdData = ItemAPIDAOImpl.attributeMetadata
                .get(itemAPIDAOImpl.getFormattedItemType(itemType))
                .get(attributeId);
        if (attributeIdData != null)
            return attributeIdData.get("ExternalKey");
        return "";
    }

    private Map<String, String> setStandardFieldValues(List<String> standardFieldList, Row row) throws
            ExportStagingException {
        Map<String, String> standardValueMap = new HashMap<>();
        for (String attributeId : standardFieldList) {
            try {
                standardValueMap.put(attributeId, getStandardFieldValueFromDB(attributeId, row));
            } catch (Exception exception) {
                System.out.println("Exception occurred - setStandardFieldValues() " + exception.getMessage());
            }
        }
        return standardValueMap;
    }

    private Map<String, Map<Long, String>> setFormattedAndValues(List<Long> attributeIdList, Row row)
            throws ExportStagingException {
        Map<String, Map<Long, String>> formattedAndValueMap = new HashMap<>();
        Map<Long, String> formattedValueMap = new HashMap<>();
        Map<Long, String> valuesMap = new HashMap<>();
        ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        for (long attributeId : attributeIdList) {
            try {
                String value = attributeId + ":Value";
                if (columnDefinitions.contains(value)) {
                    valuesMap.put(attributeId, row.getString(value));
                    String formattedValue = attributeId + ":FormattedValue";
                    if (columnDefinitions.contains(formattedValue)) {
                        formattedValueMap.put(attributeId, row.getString(formattedValue));
                    } else {
                        System.out.println("WARN | " + formattedValue + " column not available in " + super.itemType + " table");
                    }
                } else {
                    System.out.println("WARN | " + value + " column not available in " + super.itemType + " table");
                }
            } catch (IllegalArgumentException e) {
                System.out.println("Exception while fetching data from resultSet: Cause: " + e.getMessage());
            } catch (Exception e) {
                System.out.println("Exception occurred - setFormattedAndValues()" + e.getMessage());
            }
        }
        formattedAndValueMap.put("FormattedValue", formattedValueMap);
        formattedAndValueMap.put("Value", valuesMap);
        return formattedAndValueMap;
    }

    private List<String> getColumns(String itemType) throws ExportStagingException {
        return itemAPIDAOImpl.getColumns(itemType);
    }

    private String getStandardFieldValueFromDB(String attributeId, Row row) throws ExportStagingException {
        String Value = null;
        try {
            Value = itemAPIDAOImpl.getFieldValueFromResultset(row, attributeId);
        } catch (Exception e) {
            System.out.println("Exception occurred - getStandardFieldValueFromDB() " + e.getMessage());
        }
        return Value;
    }

    private List<Long> getAttributesIdList(List<Long> classList, String itemType)
            throws ExportStagingException {
        itemMappingData.putIfAbsent(itemType, new HashMap<>());
        Set<Long> attributeIDSet = new HashSet<>();
        List<Long> attributeIDListTemp;
        if (classList.size() == 0) {
            attributeIDListTemp = itemAPIDAOImpl.getAttributesIdListFromDB(0, itemType);
            attributeIDSet.addAll(attributeIDListTemp);
        }
        for (long classID : classList) {
            attributeIDListTemp = itemMappingData.get(itemType).get(classID);
            if (attributeIDListTemp == null) {
                attributeIDListTemp = itemAPIDAOImpl.getAttributesIdListFromDB(classID, itemType);
                itemMappingData.get(itemType).put(classID, attributeIDListTemp);
            }
            attributeIDSet.addAll(attributeIDListTemp);
        }
        List<Long> attributeIDList = new ArrayList<>();
        attributeIDList.addAll(attributeIDSet);
        return attributeIDList;
    }

    private List<Long> getAttributesIdList(long classID, String itemType)
            throws ExportStagingException {
        return getAttributesIdList(Arrays.asList(classID), itemType);
    }

    public Reference setReferenceData(Row row, String itemType) throws ExportStagingException {
        Reference reference = new Reference();
        long attributeID = row.getLong("AttributeID");
        reference.setAttributeID(attributeID);
        reference.setItemID(row.getLong("ItemID"));
        reference.setItemType(row.getString("SourceType"));
        reference.setLanguageID(row.getLong("LanguageID"));
        reference.setReferenceID(row.getLong("CSReferenceID"));
        reference.setSortOrder(row.getLong("SortOrder"));
        reference.setTargetID(row.getLong("TargetID"));
        reference.setTargetType(row.getString("TargetType"));
        reference.setExternalKey(row.getString("ExternalKey"));
        reference.setConfigurationID(row.getLong("ClassID"));

        List<Long> attributeIdList = new ArrayList<>();
        long configurationID = reference.getConfigurationID();
        if (configurationID != 0) {
            attributeIdList.addAll(getAttributesIdList(configurationID, reference.getTargetType()));
            reference.setAttributeIDs(attributeIdList);
        }
        Map<Long, String> valueMapListReference = setReferenceValuesMap(row, itemType, attributeIdList);
        Map<Long, String> formattedValueMapListReference = setReferenceFormattedValuesMap(row, itemType, attributeIdList);
        reference.setValueMapListReference(valueMapListReference);
        reference.setFormattedValueMapListReference(formattedValueMapListReference);
        return reference;
    }

    private Map<Long, String> setReferenceFormattedValuesMap(Row row, String itemType, List<Long> attributeIdList)
            throws ExportStagingException {
        Map<Long, String> formattedValueMapListReference = new HashMap<>();
        List<Long> specialConfigurations = getSpecialConfigurations(itemType);
        for (long attributeId : attributeIdList) {
            if (!specialConfigurations.contains(attributeId)) {
                formattedValueMapListReference.put(attributeId, getFormattedValueAttribute(attributeId, row));
            }
        }
        return formattedValueMapListReference;
    }

    private Map<Long, String> setReferenceValuesMap(Row row, String itemType, List<Long> attributeIdList)
            throws ExportStagingException {
        Map<Long, String> valueMapListReference = new HashMap<>();
        List<Long> specialConfigurations = getSpecialConfigurations(itemType);
        for (long attributeId : attributeIdList) {
            if (!specialConfigurations.contains(attributeId)) {
                valueMapListReference.put(attributeId, getValueAttribute(attributeId, row));
            }
        }
        return valueMapListReference;
    }

    public Subtable setSubtableData(Row row, String itemType) throws ExportStagingException {
        Subtable subtable = new Subtable();
        long attributeID = row.getLong("AttributeID");
        subtable.setAttributeID(attributeID);
        subtable.setCSItemTableID(row.getLong("ItemTableID"));
        subtable.setItemID(row.getLong("ItemID"));
        subtable.setItemType(row.getString("ItemType"));
        subtable.setLanguageID(row.getLong("LanguageID"));
        subtable.setSortOrder(row.getLong("SortOrder"));
        subtable.setExternalKey(row.getString("ExternalKey"));
        subtable.setConfigurationID(row.getLong("ClassID"));
        subtable.setSubtableIDs(row.getString("_SubtableIDs"));
        subtable.setReferenceIDs(row.getString("_ReferenceIDs"));
        List<Long> attributeIdList = new ArrayList<>();
        if (subtable.getConfigurationID() != 0) {
            attributeIdList = getAttributesIdList(subtable.getConfigurationID(), itemType);
        }
        subtable.setAttributeIDs(attributeIdList);
        List<Long> standardConfigurationIDs = subtable.getAttributeIDs();
        subtable.setValueMapListSubTable(setSubTableValuesMap(row, itemType, standardConfigurationIDs));
        subtable.setFormattedValueMapListSubTable(setSubTableFormattedValuesMap(row, itemType, standardConfigurationIDs));
        subtable.setExportValues(innerExportValues(row, standardConfigurationIDs));

        return subtable;
    }

    private ExportValues innerExportValues(Row row, List<Long> standardConfigurationIDs) throws ExportStagingException {
        ExportValues exportValues = new ExportValues();
        if (row.getString("_ReferenceIDs") != null) {
            List<Long> referenceTypeAttributeIds = itemAPIDAOImpl.getReferenceConfigurationIDs(row.getString("ItemType"));
            referenceTypeAttributeIds.retainAll(standardConfigurationIDs);

            exportValues.setReferenceList(
                    itemAPIDAOImpl.getReferenceData(
                            itemAPIDAOImpl.getFieldValueFromResultset(row, "ItemID"),
                            row.getString("ItemType"),
                            itemAPIDAOImpl.getFieldValueFromResultset(row, "LanguageID"),
                            itemAPIDAOImpl.getFieldValueFromResultset(row, "ItemTableID"),
                            referenceTypeAttributeIds
                    ));
        }
        if (row.getString("_SubtableIDs") != null) {
            List<Long> cSTableTypeAttributeIds = itemAPIDAOImpl.getSubtableConfigurationIDs(row.getString("ItemType"));
            cSTableTypeAttributeIds.retainAll(standardConfigurationIDs);
            exportValues.setSubtableList(
                    itemAPIDAOImpl.getSubTableData(
                            row.getLong("ItemID"),
                            row.getString("ItemType"),
                            row.getLong("LanguageID"),
                            row.getLong("ItemTableID"),
                            cSTableTypeAttributeIds
                    ));
        }

        List<AttributeValue> attributeValues = new ArrayList<>();
        for (long attributeId : standardConfigurationIDs) {
            AttributeValue attributeValue = new AttributeValue();
            attributeValue.setId(String.valueOf(attributeId));
            attributeValue.setLanguageID(Integer.parseInt(itemAPIDAOImpl.getFieldValueFromResultset(row, "LanguageID")));
            attributeValue.setValue(getValueAttribute(attributeId, row));
            attributeValue.setFormattedValue(getFormattedValueAttribute(attributeId, row));
            attributeValues.add(attributeValue);
        }
        if (attributeValues.size() > 0) {
            exportValues.setAttributeList(attributeValues);
        }
        return exportValues;
    }

    private Map<Long, String> setSubTableValuesMap(Row row, String itemType, List<Long> attributeIdList)
            throws ExportStagingException {
        Map<Long, String> valueMapListSubTable = new HashMap<>();
        List<Long> specialConfigurations = getSpecialConfigurations(itemType);
        for (long attributeId : attributeIdList) {
            if (!specialConfigurations.contains(attributeId)) {
                valueMapListSubTable.put(attributeId, getValueAttribute(attributeId, row));
            }
        }
        return valueMapListSubTable;
    }

    private Map<Long, String> setSubTableFormattedValuesMap(Row row, String itemType, List<Long> attributeIdList)
            throws ExportStagingException {
        Map<Long, String> formattedValueMapListSubTable = new HashMap<>();
        List<Long> specialConfigurations = getSpecialConfigurations(itemType);
        for (long attributeId : attributeIdList) {
            if (!specialConfigurations.contains(attributeId)) {
                formattedValueMapListSubTable.put(attributeId, getFormattedValueAttribute(attributeId, row));
            }
        }
        return formattedValueMapListSubTable;
    }

    private String getValueAttribute(long attributeId, Row row) throws ExportStagingException {
        String attributeIdInFormat = attributeId + ":Value";
        String value = null;
        try {
            value = row.getString(attributeIdInFormat);
        } catch (Exception e) {
            System.out.println("Exception occurred - getValueAttribute() " + e.getMessage());
        }
        return value;
    }

    private String getFormattedValueAttribute(long attributeId, Row row) throws ExportStagingException {
        String attributeIdInFormate = attributeId + ":FormattedValue";
        String formattedValue = null;
        try {
            formattedValue = row.getString(attributeIdInFormate);
        } catch (Exception e) {
            System.out.println("Exception occurred - getFormattedValueAttribute()" + e.getMessage());
        }
        return formattedValue;
    }

    /**
     * To get the list of Item IDs from MaterializeView, should use this API.
     *
     * @param itemType             type of an Item
     * @param searchFilterCriteria filter criteria that should be executed on MaterializedView.
     * @return list of result of IDs.
     * @throws ExportStagingException in case of exception exportstaging exception will be thrown.
     */
    public Set<Long> getItemIdsByFilterFromMaterializeView(String itemType, SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException {
        Set<Long> itemIdSet = new HashSet<>();
        try {
            String filter = setLanguageFilter(searchFilterCriteria, itemType);
            int filterColumnEndIndex = filter.indexOf(" ", filter.indexOf(" ") + 1);
            String filterColumn = filter.substring(1, filterColumnEndIndex).replace("\"", "");
            itemIdSet = itemAPIDAOImpl.getItemIdFromMaterializedView(itemType, filterColumn, filter);
        } catch (Exception e) {
            String eMessage = e.getMessage();
            System.out.println("Exception occurred - getItemIdsByFilters()" + eMessage);
            throw new ExportStagingException(eMessage);
        }
        return itemIdSet;
    }

    private String setLanguageFilter(SearchFilterCriteria searchFilterCriteria, String itemType)
            throws ExportStagingException {
        if (searchFilterCriteria.getLanguageId() == null && !RecordAPIs.ITEM_TYPE_WORKFLOW.equalsIgnoreCase(itemType)) {
            if (searchFilterCriteria.getLanguageShortName() == null) {
                searchFilterCriteria.setLanguageId(String.valueOf(getDefaultLanguageID()));
            } else {
                searchFilterCriteria.setLanguageId(String.valueOf(itemAPIDAOImpl.getLanguageID(searchFilterCriteria.getLanguageShortNameValue())));
            }
        }
        return searchFilterCriteria.getFilters();
    }


    public long getDefaultLanguageID() {
        return itemAPIDAOImpl.getDefaultLanguageID();
    }

    public Map<Integer, String> getLanguageShortNames() {
        return itemAPIDAOImpl.getLanguageShortNames();
    }

    public Set<Integer> getLanguageIDs() {
        return itemAPIDAOImpl.getLanguageIDs();
    }

    public Map<String, String> getLanguageData(int languageID) {
        return itemAPIDAOImpl.getLanguageData(languageID);
    }

    /**
     * Returns List of Item IDs according to the Filter and the Item Type.
     *
     * @param itemType             : The String which has the Item Type. Default its NULL.
     * @param searchFilterCriteria : The SearchFilterCriteria class object which has the Filters for
     *                             searching the Item IDs.
     * @return itemIdSet: HashSet The Unique Set of Item IDs.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */
    public Set<Long> getItemIdsByFilters(String itemType, SearchFilterCriteria searchFilterCriteria)
            throws ExportStagingException {
        Set<Long> itemIdSet = new HashSet<>();
        try {
            String filter = setLanguageFilter(searchFilterCriteria, itemType);
            itemIdSet = itemAPIDAOImpl.getItemIDsFromDB(itemType, filter);
        } catch (Exception e) {
            String eMessage = e.getMessage();
            System.out.println("Exception occurred - getItemIdsByFilters()" + eMessage);
            throw new ExportStagingException(eMessage);
        }
        return itemIdSet;
    }

    public String getLanguageShortName() {
        return languageShortName;
    }

    /**
     * Returns an Attribute Object containing all of the data according to the
     * Attribute ID and the Item Type.
     *
     * @param type        : The String which has the Item Type. Default its NULL.
     * @param attributeID : The long which has the ID of Attribute.
     * @return Attribute: The Attribute Class Object which has all data for the
     * Attributes.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */
    public Attribute getAttributeByID(String type, long attributeID) throws ExportStagingException {
        return getAndPrepareAttributeObject(type, attributeID);
    }

    /**
     * Returns an Attribute Object containing all of the data according to the
     * Attribute External Key and the Item Type.
     *
     * @param itemType             : The String into which has the Item Type. Default its NULL.
     * @param attributeExternalkey : The String which has the External Key for the Attribute.
     * @return Attribute: The Attribute Class Object which has all data for the
     * Attributes.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */
    public Attribute getAttributeByExternalKey(String itemType, String attributeExternalkey)
            throws ExportStagingException {
        long attributeID;
        if (ItemAPIDAOImpl.attributeExternalKey.get(itemAPIDAOImpl.getFormattedItemType(itemType)).isEmpty()) {
            return null;
        }
        attributeID = ItemAPIDAOImpl.attributeExternalKey.get(itemAPIDAOImpl.getFormattedItemType(itemType))
                .get(attributeExternalkey);
        if (attributeID == 0) {
            return null;
        }
        return getAndPrepareAttributeObject(itemType, attributeID);
    }

    private Attribute getAndPrepareAttributeObject(String itemType, long attributeID) {

        if (itemType == null) {
            return null;
        }
        itemType = itemAPIDAOImpl.getFormattedItemType(itemType);
        HashMap<String, String> attributeData = ItemAPIDAOImpl.attributeMetadata.get(itemType).get(attributeID);
        if (attributeData == null) {
            return null;
        }

        Attribute attributeConfiguration = new Attribute();
        attributeConfiguration.setId(attributeData.get("ID"));
        attributeConfiguration.setLabel(attributeData.get("Label"));
        attributeConfiguration.setExternalKey(attributeData.get("ExternalKey"));
        attributeConfiguration.setPaneTitle(attributeData.get("PaneTitle"));
        attributeConfiguration.setSectionTitle(attributeData.get("SectionTitle"));
        attributeConfiguration.setDefaultValue(attributeData.get("DefaultValue"));
        attributeConfiguration.setDescription(attributeData.get("Description"));
        attributeConfiguration.setType(attributeData.get("Type"));
        attributeConfiguration.setTypeID(attributeData.get(ExportMiscellaneousUtils.getExportDatabaseFieldTypeId()));
        attributeConfiguration.setIsClass(attributeData.get("IsLink"));
        attributeConfiguration.setInherited(attributeData.get("IsInherited"));
        attributeConfiguration.setIsFolder(attributeData.get("IsFolder"));
        attributeConfiguration.setLanguageDependency(attributeData.get("LanguageDependent"));
        attributeConfiguration.setPropertyValue(attributeData.get("PropertyValues"));
        attributeConfiguration.setParamIfNull().put("ParamA", attributeData.get("ParamA"));
        attributeConfiguration.setParamIfNull().put("ParamB", attributeData.get("ParamB"));
        attributeConfiguration.setParamIfNull().put("ParamC", attributeData.get("ParamC"));
        attributeConfiguration.setParamIfNull().put("ParamD", attributeData.get("ParamD"));
        attributeConfiguration.setParamIfNull().put("ParamE", attributeData.get("ParamE"));
        attributeConfiguration.setParamIfNull().put("ParamF", attributeData.get("ParamF"));
        attributeConfiguration.setParamIfNull().put("ParamG", attributeData.get("ParamG"));
        attributeConfiguration.setParamIfNull().put("ParamH", attributeData.get("ParamH"));
        attributeConfiguration.setParamIfNull().put("ParamI", attributeData.get("ParamI"));
        attributeConfiguration.setParamIfNull().put("ParamJ", attributeData.get("ParamJ"));
        attributeConfiguration.setSortOrder(attributeData.get("SortOrder"));
        attributeConfiguration.setTags(attributeData.get("Tags"));
        attributeConfiguration.setFieldValueMapping(attributeData);

        return attributeConfiguration;
    }

    /**
     * Returns The Attribute IDs which is in a specific class.
     *
     * @param classID  : The int which has the class ID.
     * @param itemType : The String which has the itemType.
     * @return : attributeIdList: ArrayList The Attribute IDs which is in a specific class.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */

    public List<Long> getAttributeIDsByClassID(long classID, String itemType) throws ExportStagingException {
        List<Long> attributeIdList;
        attributeIdList = getAttributesIdList(classID, itemType);
        return attributeIdList;
    }

    /**
     * Returns an Attribute Object containing all of the data according to the
     * Attribute ID/Name, language and the depth.
     *
     * @param attributeID The long which has the ID or Name of Attribute.
     * @param depth       The integer which has the depth of Attribute, By default is 0.
     * @param itemType    The String which has the ItemType of the Item
     * @return Attribute: The Attribute Class Object which has all data for the
     * Attributes.
     * @throws ExportStagingException If any type of Exception will throw Handle by this Custom
     *                                Exception
     */

    public List<Attribute> getAttribute(long attributeID, int depth, String itemType)
            throws ExportStagingException {
        itemType = itemAPIDAOImpl.getFormattedItemType(itemType);
        List<Attribute> attributes = new ArrayList<>();
        List<Long> rootAttributeList = new ArrayList<>();
        if (attributeID == 0) {
            rootAttributeList = ItemAPIDAOImpl.attributesParentChildMapping.get(itemType).get(0L);
        } else {
            if (ItemAPIDAOImpl.attributesParentChildMapping.get(itemType).containsKey(attributeID)) {
                rootAttributeList.add(attributeID);
            }
        }
        for (long ID : rootAttributeList) {
            Attribute attribute = preparedAndGetAttribute(ID, depth, itemType);
            attributes.add(attribute);
        }
        return attributes;
    }

    private Attribute preparedAndGetAttribute(long attributeID, int depth, String itemType)
            throws ExportStagingException {

        itemType = itemAPIDAOImpl.getFormattedItemType(itemType);
        int level;
        long currentNode;
        List<Long> attributeIterator = new ArrayList<>();
        Map<Long, Integer> attributeLevel = new LinkedHashMap<>();
        Map<Long, Attribute> attributeMetaDataObject = new LinkedHashMap<>();

        attributeIterator.add(attributeID);
        Attribute attributeObject = getAttributeByID(itemType, attributeID);
        attributeLevel.put(attributeID, 0);
        attributeMetaDataObject.put(attributeID, attributeObject);

        do {
            List<Attribute> childList = new ArrayList<>();
            currentNode = attributeIterator.get(0);
            List<Long> child = ItemAPIDAOImpl.attributesParentChildMapping.get(itemType).get(currentNode);
            level = attributeLevel.get(currentNode);
            if (depth != 0 && level >= depth)
                break;
            if (child != null) {
                for (long attribute : child) {
                    attributeObject = getAttributeByID(itemType, attribute);
                    attributeMetaDataObject.put(attribute, attributeObject);
                    attributeLevel.put(attribute, (level + 1));
                    childList.add(attributeObject);
                    attributeIterator.add(attribute);
                }
                attributeMetaDataObject.get(currentNode).setAttribute(childList);
            }
            attributeIterator.remove(0);
        }
        while (!(attributeIterator.isEmpty()));
        return attributeMetaDataObject.get(attributeID);
    }

    protected void finalize() {
        super.finalize();
    }

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        this.context = context;

    }
}
