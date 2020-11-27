package com.exportstaging.api.dao;

import com.datastax.driver.core.*;
import com.exportstaging.api.domain.Item;
import com.exportstaging.api.domain.Reference;
import com.exportstaging.api.domain.Subtable;
import com.exportstaging.api.exception.ExportMaterializeViewException;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.implementation.ItemApiImpl;
import com.exportstaging.api.implementation.WrapperImpl;
import com.exportstaging.api.wraper.ItemWrapper;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.connectors.database.CassandraDAO;
import com.exportstaging.connectors.database.DatabaseConnection;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository("itemAPIDAOImpl")
public class ItemAPIDAOImpl implements InitializingBean, ApplicationContextAware {

    @Autowired
    private DatabaseConnection databaseConnection;
    @Autowired
    protected CassandraDAO cassandraDAO;

    @JsonIgnore
    @Autowired
    private ItemApiImpl itemApiImpl;

    @Value("${cassandra.prefix.export}")
    private String prefixExport;
    @Value("${cassandra.table.item.id}")
    private String itemID;
    @Value("${cassandra.table.item.externalkey}")
    private String externalKey;
    @Value("${cassandra.suffix.view}")
    private String itemTypeSuffix;
    @Value("${export.core.itemtypes}")
    private String itemTypes;
    @Value("${export.core.recordtypes}")
    private String recordTypes;
    @Value("${cassandra.suffix.configuration}")
    private String sSuffixConfiguration;
    @Value("${cassandra.suffix.mapping}")
    private String sSuffixMapping;
    @Value("${cassandra.suffix.reference}")
    private String sSuffixReference;
    @Value("${cassandra.suffix.subtable}")
    private String sSuffixSubtable;
    @Value("${cassandra.suffix.language}")
    private String sSuffixLanguage;

    private static final String ATTRIBUTE_TYPE_STANDARD = "1";
    private static final String ATTRIBUTE_TYPE_REFERENCE = "2";
    private static final String ATTRIBUTE_TYPE_SUBTABLE = "3";
    private static final String ATTRIBUTE_TYPE_FOLDER = "0";
    private static final int READ_TIMEOUT_IN_MILISEC = 1800000;

    private ApplicationContext context = null;
    public static Map<String, HashMap<Long, HashMap<String, String>>> attributeMetadata = new HashMap<>();
    public static Map<String, HashMap<Long, List<Long>>> attributesParentChildMapping = new LinkedHashMap<>();
    public static Map<String, HashMap<String, Long>> attributeExternalKey = new HashMap<>();
    private static Map<String, HashMap<Long, Long>> attributeTypeID = new HashMap<>();
    public static Map<String, Integer> languageIDMap = new HashMap<>();
    private static Map<String, HashMap<String, List<Long>>> attributeTypeIdWithAttributeIdsMapping = new HashMap<>();
    public String defaultShortName;
    private long defaultLanguageID;

    String ITEM_TYPE_PRODUCT = "Pdmarticle";
    String ITEM_TYPE_VIEW = "Pdmarticlestructure";

    private Map<String, Map<String, String>> columnFamily = new HashMap<>();

    public String getFormattedItemType(String itemType) {
        if (itemType.contains(itemTypeSuffix)) {
            itemType = itemType.substring(0, itemType.length() - 9);
        }
        return itemType;
    }

    public ItemAPIDAOImpl() {
    }

    private void initialItemAPIDAOImpl() {
        if (databaseConnection == null)
            databaseConnection = (DatabaseConnection) context.getBean("cassandraConnection");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initialItemAPIDAOImpl();
    }

    public ItemAPIDAOImpl getInstance() {
        return (ItemAPIDAOImpl) context.getBean("itemAPIDAOImpl");
    }

    public void init(String itemType) throws ExportStagingException {
        setColumnFamily(itemType);
        if (itemTypes.contains(itemType)) {
            getAttributeMetadata(itemType);
            setAttributeTypeID(itemType);
        }
        if (defaultShortName == null) {
            populateLanguageIDMapAndSetDefaultShortName();
        }
    }

    public List<String> getColumns(String itemType) throws ExportStagingException {
        List<String> columnList = new ArrayList<>();
        try {
            columnList = cassandraDAO.getColumns(getTableName(itemType));
        } catch (ExportStagingException e) {
        }
        return columnList;
    }

    @JsonIgnore
    private void populateLanguageIDMapAndSetDefaultShortName() throws ExportStagingException {
        String selectQuery = "SELECT \"ShortName\",\"ID\",\"IsDefault\" FROM  " + databaseConnection.getKeyspace() + "." +
                getTableName(sSuffixLanguage);
        ResultSet resultSet = databaseConnection.getSession().execute(selectQuery);
        List<Row> languageIds = resultSet.all();
        if (languageIds.isEmpty()) {
            throw new ExportStagingException("Message: export_language table is empty");
        }
        for (Row row : languageIds) {
            int langID = row.getInt("ID");
            String shortName = row.getString("ShortName");
            languageIDMap.put(shortName, langID);
            if (row.getInt("IsDefault") == 1) {
                if (shortName != null && !shortName.equals("")) {
                    setDefaultShortName(shortName);
                    setDefaultLanguageID(langID);
                }
            }
        }
    }

    private void setDefaultLanguageID(int languageID) {
        this.defaultLanguageID = languageID;
    }

    public long getDefaultLanguageID() {
        return this.defaultLanguageID;
    }

    public Map<Integer, String> getLanguageShortNames() {
        return ExportMiscellaneousUtils.reverseMap(languageIDMap);
    }

    public Set<Integer> getLanguageIDs() {
        return new HashSet<>(languageIDMap.values());
    }

    private void setAttributeTypeID(String itemType) {
        String query;
        ResultSet resultSet;
        try {
            query = "SELECT \"ID\",\"" + ExportMiscellaneousUtils.getExportDatabaseFieldTypeId() + "\" FROM " + databaseConnection.getKeyspace() + "." +
                    getTableName(itemType, sSuffixConfiguration) + ";";
            resultSet = this.databaseConnection.getSession().execute(query);
            List<Row> resultRow = resultSet.all();
            String itemTypeFormatted = getFormattedItemType(itemType);
            for (Row aResultRow : resultRow) {
                attributeTypeID.get(itemTypeFormatted).put(aResultRow.getLong("ID"), aResultRow.getLong(ExportMiscellaneousUtils.getExportDatabaseFieldTypeId()));
            }
        } catch (Exception e) {
            System.out.println("Exception occurred : setAttributeTypeID() " + e.getMessage());
        }
    }

    public List<Long> getAttributeFolderIDs(String itemType) throws ExportStagingException {
        return getSpecialAttributeIDs(itemType, ATTRIBUTE_TYPE_FOLDER);
    }

    public List<Long> getSubtableConfigurationIDs(String itemType) throws ExportStagingException {
        return getSpecialAttributeIDs(itemType, ATTRIBUTE_TYPE_SUBTABLE);
    }

    public List<Long> getReferenceConfigurationIDs(String itemType) throws ExportStagingException {
        return getSpecialAttributeIDs(itemType, ATTRIBUTE_TYPE_REFERENCE);
    }

    private List<Long> getSpecialAttributeIDs(String itemType, String typeID) throws ExportStagingException {
        List<Long> specialConfigurationList = new ArrayList<>();
        String formattedItemType = getFormattedItemType(itemType);
        try {
            attributeTypeIdWithAttributeIdsMapping.putIfAbsent(formattedItemType, new HashMap<>());
            if (attributeTypeIdWithAttributeIdsMapping.get(formattedItemType).get(typeID) == null) {

                String selectQuery = "SELECT \"" + itemID + "\"" +
                        " FROM  " + databaseConnection.getKeyspace() + "." + getTableName(itemType, sSuffixConfiguration) +
                        " WHERE \"" + ExportMiscellaneousUtils.getExportDatabaseFieldModule() + "\" ='" + getFormattedItemType(itemType) +
                        "' AND \"" + ExportMiscellaneousUtils.getExportDatabaseFieldTypeId() + "\" = " + typeID +
                        " ALLOW FILTERING;";

                ResultSet resultSet = databaseConnection.getSession().execute(selectQuery);
                for (Row row : resultSet) {
                    specialConfigurationList.add(row.getLong(itemID));
                }

                attributeTypeIdWithAttributeIdsMapping.get(formattedItemType).put(typeID, specialConfigurationList);
            }
        } catch (Exception e) {
            throw new ExportStagingException("Exception while fetching attributeIds for " + itemType + " with _TypeID " + typeID
                    + "Exception message: " + e.getMessage());
        }
        return new ArrayList<>(attributeTypeIdWithAttributeIdsMapping.get(formattedItemType).get(typeID));
    }

    private Map<String, HashMap<Long, HashMap<String, String>>> getAttributeMetadata(String itemType) {
        String query;
        ResultSet resultSet;
        Map<String, String> attributeProperty = new HashMap<>();
        try {
            query = "SELECT * FROM " + databaseConnection.getKeyspace() + "." + getTableName(itemType, sSuffixConfiguration) + ";";
            resultSet = this.databaseConnection.getSession().execute(query);
            List<Row> attIDItr1 = resultSet.all();
            String formattedItemType = getFormattedItemType(itemType);

            if (!attributeMetadata.containsKey(formattedItemType)) {
                attributeMetadata.put(formattedItemType, new HashMap<>());
                attributeExternalKey.put(formattedItemType, new HashMap<>());
                attributeTypeID.put(formattedItemType, new HashMap<>());
                attributesParentChildMapping.put(formattedItemType, new HashMap<>());
            }

            for (Row classAttribute : attIDItr1) {
                long AttributeID = classAttribute.getLong(itemID);
                attributeMetadata.get(formattedItemType).putIfAbsent(AttributeID, new HashMap<>());
                ColumnDefinitions property = classAttribute.getColumnDefinitions();
                int propertySize = property.size();
                for (int i = 0; i < propertySize; i++) {
                    String fieldValue = getFieldValueFromResultset(classAttribute, property.getName(i));
                    attributeProperty.put(property.getName(i), fieldValue);
                }
                attributeMetadata.get(formattedItemType).get(AttributeID).putAll(attributeProperty);

                String externalKey = classAttribute.getString("ExternalKey");
                if (externalKey != null) {
                    if (!(externalKey.isEmpty() || externalKey.equals(""))) {
                        attributeExternalKey.get(formattedItemType).put(externalKey, AttributeID);
                    }
                }
                long parentID = classAttribute.getLong("ParentID");
                int isLink = classAttribute.getInt("IsLink");
                if (isLink == 0) {
                    attributesParentChildMapping.get(formattedItemType).putIfAbsent(AttributeID, new ArrayList<>());
                    attributesParentChildMapping.get(formattedItemType).putIfAbsent(parentID, new ArrayList<>());
                    attributesParentChildMapping.get(formattedItemType).get(parentID).add(AttributeID);
                }
            }
        } catch (Exception e) {
            System.out.println("Error ES1001 - Unable to create AttributeMetadata: " + e.getMessage());
        }
        return attributeMetadata;
    }

    @JsonIgnore
    public Set<Long> getItemChildrenById(long itemId, int level, String itemType) throws ExportStagingException {
        int levelCount = 1;
        Set<Long> itemIds;
        Set<Long> immediateItemIds;
        Set<Long> immediateItemIds1 = new HashSet<>();
        Set<Long> immediateItemIds2 = new HashSet<>();
        itemIds = getChildrenById(itemId, itemType);
        if (level == levelCount)
            return itemIds;
        else {
            immediateItemIds1.addAll(itemIds);
            while (true) {
                levelCount++;
                for (long child : immediateItemIds1) {
                    immediateItemIds = getChildrenById(child, itemType);
                    itemIds.addAll(immediateItemIds);
                    immediateItemIds2.addAll(immediateItemIds);
                }
                if (level == levelCount || (level == 0 && immediateItemIds2.isEmpty()))
                    break;
                immediateItemIds1.clear();
                immediateItemIds1.addAll(immediateItemIds2);
                immediateItemIds2.clear();
            }
            return itemIds;
        }
    }

    private Set<Long> getChildrenById(long ID, String itemType) throws ExportStagingException {
        Set<Long> itemIds = new HashSet<>();
        try {
            itemIds.clear();
            itemIds = getChild(ID, itemType);
        } catch (Exception e) {
            System.out.println("Exception occurred - getChildrenById()" + e.getMessage());
        }
        return itemIds;
    }

    @JsonIgnore
    private Set<Long> getChild(long ID, String itemType) throws ExportStagingException {
        Set<Long> children = new HashSet<>();
        String selectQuery;
        long itemId;
        try {
            selectQuery = "SELECT \"ID\" FROM  " + databaseConnection.getKeyspace() + "." + getTableName(itemType)
                    + " WHERE \"ParentID\" = " + ID + " ALLOW FILTERING;";
            ResultSet resultSet = databaseConnection.getSession().execute(selectQuery);
            for (Row row : resultSet) {
                itemId = row.getLong("ID");
                children.add(itemId);
            }
        } catch (Exception e) {
            throw new ExportStagingException("Exception while executing query and collecting child ids for Parent Id " + ID +
                    "Exception message: " + e.getMessage());
        }
        return children;
    }

    public Set<Long> getItemIDsFromDB(String itemType, String filter) throws ExportStagingException {
        try {
            Set<Long> itemIDSet = new HashSet<>();
            String preparedIDs = getListOfIds(filter);
            String QueryFilter = prepareINQqueryFilter(filter, preparedIDs);
            List<String> itemIDsList = new ArrayList<String>(Arrays.asList(preparedIDs.split(",")));
            Collection<List<String>> batchItemIDsList = Lists.partition(itemIDsList, 1000);
            String selectQuery;
            //TODO get IDs from elasticsearch instead of cassandra
            for (List batchItemIds : batchItemIDsList) {
                selectQuery = "SELECT \"ID\" "
                        + " FROM " + databaseConnection.getKeyspace() + ".export_" + itemType
                        + " WHERE " + QueryFilter.replace("XXX", String.join(",", batchItemIds)) + " ALLOW FILTERING";
                ResultSet resultSet = getResultSet(selectQuery);
                for (Row row : resultSet) {
                    itemIDSet.add(row.getLong(itemID));
                }
            }

            return itemIDSet;

        } catch (Exception e) {
            String exception = e.getMessage();
            throw new ExportStagingException("[Export Exception]:" +
                    " exeception occured while fetching data from " + itemType + " cassandra table." + exception);
        }
    }

    @JsonIgnore
    public ItemWrapper getParentData(long itemId, String itemType) throws ExportStagingException {
        return prepareAndSetItemDataFromDB(itemType, itemID, String.valueOf(itemId));
    }

    private String prepareINQqueryFilter(String columnFilter, String preparedIDs) {
        if (columnFilter.contains("IN")) {
            columnFilter = columnFilter.replace(preparedIDs, "XXX");
            return columnFilter;
        }
        return columnFilter;
    }

    private String getListOfIds(String columnFilter) {
        if (columnFilter.contains(" IN ")) {
            return columnFilter.substring(columnFilter.indexOf("(") + 1, columnFilter.indexOf(")"));
        }
        return columnFilter;
    }

    public Set<Long> getItemIdFromMaterializedView(String itemType, String viewColumnName, String columnFilter)
            throws ExportStagingException {
        String selectViewQuery;
        Set<Long> itemIDSet = new HashSet<>();
        String preparedIDs = getListOfIds(columnFilter);
        String QueryFilter = prepareINQqueryFilter(columnFilter, preparedIDs);
        List<String> itemIDsList = new ArrayList<String>(Arrays.asList(preparedIDs.split(",")));
        Collection<List<String>> batchItemIDsList = Lists.partition(itemIDsList, 1000);
        try {
            for (List batchItemIds : batchItemIDsList) {
                selectViewQuery = "SELECT \"ID\" "
                        + " FROM " + databaseConnection.getKeyspace() + ".export_" + itemType + "_view_" + viewColumnName
                        + " WHERE " + QueryFilter.replace("XXX", String.join(",", batchItemIds));

                ResultSet resultSet = getResultSet(selectViewQuery);
                for (Row row : resultSet) {
                    itemIDSet.add(row.getLong(itemID));
                }
            }
        } catch (Exception e) {
            throw new ExportMaterializeViewException("[Export Exception]: while fetching data for " + viewColumnName + " from materialized view." + e.getMessage());
        }
        return itemIDSet;
    }

    @JsonIgnore
    public int getLanguageID(String filterLanguageShortName) throws ExportStagingException {
        if (languageIDMap != null) {
            for (Map.Entry entry : languageIDMap.entrySet()) {
                if (entry.getValue().equals(filterLanguageShortName)) {
                    return (int) entry.getKey();
                }
            }
        }
        int defaultLanguage = 0;
        int languageID = 0;
        try {
            String selectQuery = "SELECT \"ID\" FROM  " + databaseConnection.getKeyspace() + "." + getTableName(sSuffixLanguage)
                    + " WHERE \"ShortName\" ='" + filterLanguageShortName + "';";
            ResultSet resultSet = databaseConnection.getSession().execute(selectQuery);
            Iterator<Row> itr = resultSet.iterator();
            while (itr.hasNext()) {
                defaultLanguage = resultSet.one().getInt("ID");
            }
            if (defaultLanguage != 0) {
                languageID = defaultLanguage;
            }
        } catch (Exception e) {
            System.out.println("Exception occurred - getLanguageID()" + e.getMessage());
        }
        return languageID;
    }

    public Map<String, String> getLanguageData(int languageID) {
        Map<String, String> languageData = new HashMap<>();
        try {
            String selectQuery = "SELECT * FROM " + databaseConnection.getKeyspace() + "." + getTableName(sSuffixLanguage) + " WHERE \"ID\" = " + languageID;
            ResultSet resultSet = cassandraDAO.dbOperations(selectQuery);
            List<Row> rows = resultSet.all();
            ColumnDefinitions definitions = resultSet.getColumnDefinitions();
            for (Row row : rows) {
                for (ColumnDefinitions.Definition definition : definitions) {
                    languageData.put(definition.getName(), String.valueOf(row.getObject(definition.getName())));
                }
            }
        } catch (Exception e) {
            System.out.println("Exception occurred - getLanguageData for languageID " + languageID + ". Error cause: " + e.getMessage());
        }
        return languageData;
    }

    public Map<Long, HashMap<Long, Item>> setItemTableData(String itemIDs, String itemType, String filterLanguageID) throws ExportStagingException {
        String selectQuery;
        ResultSet resultSet;
        Map<Long, HashMap<Long, Item>> itemsData = new HashMap<>();
        try {
            List<String> itemIDsList = new ArrayList<String>(Arrays.asList(itemIDs.split(",")));
            Collection<List<String>> batchItemIDsList = Lists.partition(itemIDsList, 1000);
            Statement simpleStatement;
            for (List batchItemIDs : batchItemIDsList) {
                selectQuery = "SELECT * FROM  " + databaseConnection.getKeyspace() + "." + getTableName(itemType)
                        + " WHERE " + " \"ID\"" + " IN (" + String.join(",", batchItemIDs) + ")";

                if (itemTypes.contains(itemType)) {
                    selectQuery += " AND \"LanguageID\" " + filterLanguageID;
                }
                selectQuery += ";";

                simpleStatement = new SimpleStatement(selectQuery).setReadTimeoutMillis(READ_TIMEOUT_IN_MILISEC);
                resultSet = databaseConnection.getSession().execute(simpleStatement);

                for (Row row : resultSet) {
                    long itemId = row.getLong("ID");
                    long languageID = 0;
                    if (getItemTypes().contains(itemType)) {
                        languageID = row.getLong("LanguageID");
                    } else {
                        // TODO write it generic
                        if (row.getLong("StateID") == 0) {
                            languageID = getLanguageID(getDefaultShortName());
                        }
                    }
                    itemsData.putIfAbsent(itemId, new HashMap<>());
                    itemsData.get(itemId).put(languageID, itemApiImpl.setItemData(row, itemId, itemType));
                }
            }
        } catch (Exception e) {
            System.out.println("Exception occurred - setItemTableData() " + e.getMessage());
            throw new ExportStagingException(e);
        }
        return itemsData;
    }

    public ItemWrapper prepareAndSetItemDataFromDB(String itemType, String fieldName, String fieldValue)
            throws ExportStagingException {
        ItemWrapper itemLanguageList;
        boolean hasRecords = false;
        HashMap<String, Item> itemList = new HashMap<>();
        HashMap<Long, String> languageIdAndShortName = new HashMap<>();
        String selectQuery;
        String itemId = fieldValue;
        Set<Long> itemIdSet;
        String prepareConditions = "\"" + fieldName + "\"=";
        String allowFiltering = " ALLOW FILTERING";
        try {
            if (itemTypes.contains(itemType) && !fieldName.equals(itemID)) {
                prepareConditions = " \"ID\" = ";
                itemIdSet = getItemIDs(itemType, fieldName, fieldValue);
                if (itemIdSet == null || itemIdSet.isEmpty())
                    return null;
                for (Long id : itemIdSet) {
                    itemId = String.valueOf(id);
                }
            } else if (recordTypes.contains(itemType) && !fieldName.equals(itemID) && !fieldName.equals("StateID")) {
                itemId = "'" + itemId + "'";
            }
            selectQuery = "SELECT * FROM  " + databaseConnection.getKeyspace() + "." + getTableName(itemType)
                    + " WHERE " + prepareConditions + itemId + allowFiltering + ";";
            Statement statement = new SimpleStatement(selectQuery).setReadTimeoutMillis(READ_TIMEOUT_IN_MILISEC);
            statement.setFetchSize(200);
            ResultSet resultSet = databaseConnection.getSession().execute(statement);
            Item item = null;
            for (Row row : resultSet) {
                item = itemApiImpl.setItemData(row, row.getLong(itemID), itemType);
                hasRecords = true;
                String shortName = item.getLanguageShortName();
                itemList.put(shortName, item);
                languageIdAndShortName.put(item.getLanguageID(), shortName);

                if (recordTypes.contains(itemType)) {
                    break;
                }
            }

            itemLanguageList = context.getBean("wrapperImpl", WrapperImpl.class);
            itemLanguageList.setDefaultLanguageShortName(defaultShortName);
            itemLanguageList.setDefaultLanguageID((int) getDefaultLanguageID());
            itemLanguageList.setItemType(itemType);
            itemLanguageList.setLanguagesItem(itemList);
            ((WrapperImpl) itemLanguageList).setLanguageIdsShortNamesMapping(languageIdAndShortName);
        } catch (Exception e) {
            System.out.println("Exception occurred - prepareAndSetItemDataFromDB() " + e.getMessage());
            throw e;
        }
        if (hasRecords) {
            return itemLanguageList;
        } else {
            return null;
        }
    }

    private Set<Long> getItemIDs(String itemType, String fieldName, String fieldValue) throws ExportStagingException {
        String filter = "\"LanguageID\" = " + getDefaultLanguageID() + " AND \"" + fieldName + "\" = " + prepareFieldValue(fieldName, fieldValue, itemType);
        Set<Long> itemIdSet;
        try {
            itemIdSet = getItemIdFromMaterializedView(itemType, fieldName, filter);
        } catch (ExportMaterializeViewException e) {
            itemIdSet = getItemIDsFromDB(itemType, filter);
        }
        return itemIdSet;
    }

    public boolean checkItemType(String itemType) throws ExportStagingException {
        return cassandraDAO.checkTable(getTableName(itemType));
    }

    public List<Long> getWorkflowStateIDsfromDB(long itemID, String itemType) throws ExportStagingException {
        List<Long> stateIdList = new ArrayList<>();
        String selectQuery;
        long stateID = 0;
        try {
            selectQuery = "SELECT \"StateID\" FROM  " + databaseConnection.getKeyspace() + "." + getTableName(itemType)
                    + " WHERE \"ID\" =" + itemID + " ALLOW FILTERING;";
            ResultSet resultSet = databaseConnection.getSession().execute(selectQuery);
            for (Row row : resultSet) {
                stateID = row.getLong("StateID");
                if (stateID != 0) {
                    stateIdList.add(stateID);
                }
            }
        } catch (Exception e) {
            System.out.println("Unable to get StateIDs from Cassandra " + stateID + e.getMessage());
            throw e;
        }
        return stateIdList;
    }

    @JsonIgnore
    public List<Long> getAttributesIdListFromDB(long classID, String itemType)
            throws ExportStagingException {
        List<Long> attributeIdList = new ArrayList<>();
        String selectQuery;
        long attributeID = 0;
        try {
            selectQuery = "SELECT \"AttributeID\" FROM  " + databaseConnection.getKeyspace() + "." + getTableName(
                    itemType, sSuffixMapping) + " WHERE \"ClassID\" =" + classID + ";";
            ResultSet resultSet = databaseConnection.getSession().execute(selectQuery);
            for (Row row : resultSet) {
                attributeID = row.getLong("AttributeID");
                attributeIdList.add(attributeID);
            }
        } catch (Exception e) {
            System.out.println("Error ES1002 - Unable to get AttributeIDs from Cassandra " + attributeID + e.getMessage());
        }
        return attributeIdList;
    }

    @JsonIgnore
    public List<Subtable> getSubTableData(long itemID, String itemType, long languageID, long subItemID, List tableAttributeIds)
            throws ExportStagingException {
        List<Subtable> subtableRowList = new ArrayList<>();
        prepareSubtableData(itemID, itemType, languageID, subItemID, subtableRowList, tableAttributeIds);

        return subtableRowList;
    }


    private void prepareSubtableData(long itemID, String itemType, long languageID, long subItemID, List<Subtable> subtableRowList, List tableAttributeIds) throws ExportStagingException {
        String selectQuery;
        try {
            if (tableAttributeIds.size() > 0) {
                String tableName;
                String subItemIdQueryFilter = "";
                if (subItemID != 0) {
                    String itemTypeTableName = (itemType.replace("table", "")).toLowerCase();
                    tableName = getTableName(itemTypeTableName, sSuffixSubtable);

                    subItemIdQueryFilter = " AND \"" + ExportMiscellaneousUtils.getExportDatabaseFieldSubitemid() + "\" = " + subItemID +
                            " ALLOW FILTERING;";
                } else {
                    tableName = getTableName(itemType, sSuffixSubtable);
                }

                selectQuery = "SELECT * FROM  " + databaseConnection.getKeyspace() + "." + tableName +
                        " WHERE \"ItemID\" =" + itemID +
                        " AND \"ItemType\" = '" + itemType + "'" +
                        " AND \"LanguageID\"=" + languageID +
                        " AND \"AttributeID\" IN (" + getCommaSeparatedIds(tableAttributeIds) + ")" + subItemIdQueryFilter;

                ResultSet resultSet = getResultSet(selectQuery);
                for (Row row : resultSet) {
                    subtableRowList.add(itemApiImpl.setSubtableData(row, itemType));
                }
                updateBaseObjectResult(itemID, itemType, languageID, subtableRowList, tableAttributeIds);
            }
        } catch (Exception e) {
            throw new ExportStagingException("Exception while fetching and preparing data for subtable. Exception Message: " + e.getMessage());
        }
    }


    /**
     * Method will be responsible to update the pdmarticle subtable data in a provided map case request is come
     * for pdmarticlestructure
     * In future if any object pair like this should be handle here
     *
     * @param itemID          long item id
     * @param itemType        String object Type
     * @param languageID      long Language Id
     * @param subtableRowList list where subtable data is store for pdmarticlestructure
     * @throws ExportStagingException exception object if anything goes wrong
     */
    private void updateBaseObjectResult(long itemID, String itemType, long languageID, List<Subtable> subtableRowList, List tableAttributeIds) throws ExportStagingException {
        if (itemType.equalsIgnoreCase(ITEM_TYPE_VIEW)) {
            long extentionId = getExtensionIds(itemID, languageID, itemType);
            prepareSubtableData(extentionId, ITEM_TYPE_PRODUCT, languageID, extentionId, subtableRowList, tableAttributeIds);
        }
    }


    /**
     * Method will provide extension id
     * Extension id is nothing but the pdmarticle id which is currently refer in pdmarticlestructure
     *
     * @param itemId     long Item Id
     * @param languageID long language Id
     * @param itemType   String object Type
     * @return extension id/pdmarticle id which is currently refer in pdmarticlestructure
     * @throws ExportStagingException exception object if anything goes wrong
     */
    private long getExtensionIds(long itemId, long languageID, String itemType) throws ExportStagingException {
        String selectQuery = "SELECT \"_ExtensionID\" FROM "
                + databaseConnection.getKeyspace() + "." + getTableName(itemType)
                + " WHERE \"ID\" = " + itemId + " AND \"LanguageID\" = " + languageID;

        Row resultSet = databaseConnection.getSession().execute(selectQuery).one();
        long extentionId = resultSet.getLong("_ExtensionID");

        return extentionId;
    }


    @JsonIgnore
    public List<Reference> getReferenceData(String itemID, String itemType, String languageID, String subItemID, List attributeIDs)
            throws ExportStagingException {
        List<Reference> referenceList = new ArrayList<>();
        String selectQuery;
        try {
            if (attributeIDs.size() > 0) {
                String subItemIdQueryFilter = "";
                if (subItemID != null) {
                    subItemIdQueryFilter = " AND \"" + ExportMiscellaneousUtils.getExportDatabaseFieldSubitemid() + "\" = " + subItemID +
                            " ALLOW FILTERING;";
                }
                selectQuery = "SELECT * FROM  " + databaseConnection.getKeyspace() + "." + getTableName(itemType, sSuffixReference) +
                        " WHERE \"ItemID\" =" + itemID +
                        " AND \"SourceType\" = '" + itemType + "'" +
                        " AND \"LanguageID\"=" + languageID +
                        " AND \"AttributeID\" IN (" + getCommaSeparatedIds(attributeIDs) + ")" + subItemIdQueryFilter;

                ResultSet resultSet = getResultSet(selectQuery);

                for (Row row : resultSet) {
                    Reference reference = itemApiImpl.setReferenceData(row, itemType);
                    referenceList.add(reference);
                }
            }
        } catch (Exception e) {
            throw new ExportStagingException("Exception while fetching and preparing data for reference. Exception Message: " + e.getMessage());
        }
        return referenceList;
    }

    private ResultSet getResultSet(String selectQuery) throws ExportStagingException {
        Statement statement = new SimpleStatement(selectQuery).setReadTimeoutMillis(READ_TIMEOUT_IN_MILISEC);
        statement.setFetchSize(500);
        return databaseConnection.getSession().execute(statement);
    }

    public String getDefaultShortName() {
        return defaultShortName;
    }

    private void setDefaultShortName(String defaultShortName) {
        this.defaultShortName = defaultShortName;
    }

    @JsonIgnore
    public List<String> getLanguagesShortName(String itemType) throws ExportStagingException {
        List<String> languageShortNames = new ArrayList<>();
        if (languageIDMap != null) {
            for (Map.Entry<String, Integer> entry : languageIDMap.entrySet()) {
                languageShortNames.add(entry.getKey());
            }
            return languageShortNames;
        }
        try {
            String selectQuery = "SELECT \"ShortName\" FROM  " + databaseConnection.getKeyspace() + "." + getTableName(sSuffixLanguage);
            ResultSet resultSet = databaseConnection.getSession().execute(selectQuery);
            Iterator<Row> itr = resultSet.iterator();
            while (itr.hasNext()) {
                languageShortNames.add(resultSet.one().getString("ShortName"));
            }
        } catch (Exception e) {
            System.out.println("Exception occurred - getLanguageID()" + e.getMessage());
        }
        return languageShortNames;
    }

    @JsonIgnore
    public List<String> getChildren(String parentID, String itemType) {
        List<String> children = new ArrayList<>();
        try {
            String selectQuery = "SELECT \"AttributeID\" FROM  " + databaseConnection.getKeyspace() + "."
                    + getTableName(itemType, sSuffixConfiguration)
                    + " WHERE \"ParentID\" = '" + parentID + "' ALLOW FILTERING";
            ResultSet resultSet = databaseConnection.getSession().execute(selectQuery);
            Iterator<Row> itr = resultSet.iterator();
            while (itr.hasNext()) {
                children.add(resultSet.one().getString("AttributeID"));
            }
        } catch (Exception e) {
            System.out.println("Exception occurred - getChildren()" + e.getMessage());
        }
        return children;

    }

    /**
     * Key pair value of column and its data type for Item (its subtable,reference,configuration - if present)
     *
     * @param itemType Module name (i.e Pdmarticle/Mamfile/Pdmarticlestructure)
     */
    private void setColumnFamily(String itemType) {
        if (columnFamily.get(itemType) == null) {
            try {
                List<ColumnMetadata> itemColumns;
                TableMetadata itemTableMetaData = databaseConnection.getCluster().getMetadata().getKeyspace(databaseConnection.getKeyspace()).getTable(getTableName(itemType));
                if (itemTableMetaData != null) {
                    columnFamily.put(itemType, getColumnNameType(itemTableMetaData));
                }

                TableMetadata subtableMetaData = databaseConnection.getCluster().getMetadata().getKeyspace(databaseConnection.getKeyspace()).getTable(getTableName(itemType, sSuffixSubtable));
                if (subtableMetaData != null) {
                    columnFamily.get(itemType).putAll(getColumnNameType(subtableMetaData));
                }
                TableMetadata referenceMetaData = databaseConnection.getCluster().getMetadata().getKeyspace(databaseConnection.getKeyspace()).getTable(getTableName(itemType, sSuffixReference));
                if (referenceMetaData != null) {
                    columnFamily.get(itemType).putAll(getColumnNameType(referenceMetaData));
                }
                TableMetadata configurationMetaData = databaseConnection.getCluster().getMetadata().getKeyspace(databaseConnection.getKeyspace()).getTable(getTableName(itemType, sSuffixConfiguration));
                if (configurationMetaData != null) {
                    columnFamily.get(itemType).putAll(getColumnNameType(configurationMetaData));
                }
                TableMetadata mappingMetaData = databaseConnection.getCluster().getMetadata().getKeyspace(databaseConnection.getKeyspace()).getTable(getTableName(itemType, sSuffixMapping));
                if (mappingMetaData != null) {
                    columnFamily.get(itemType).putAll(getColumnNameType(mappingMetaData));
                }
            } catch (Exception e) {
                System.out.println("Export Staging Error in getColumnsWithType(): " + e.getMessage());
            }
        }
    }


    private Map<String, String> getColumnNameType(TableMetadata tableMetaData) {
        Map<String, String> columnsAndType = new HashMap<>();
        List<ColumnMetadata> itemColumns = tableMetaData.getColumns();
        for (ColumnMetadata element : itemColumns) {
            columnsAndType.put(element.getName(), element.getType().toString());
        }
        return columnsAndType;
    }

    /**
     * Add single quote on both side id column data type is text or timestamp
     *
     * @param fieldName  Name of column
     * @param fieldValue Value to be set in query
     * @param itemType   Item Type
     * @return prepare value
     */
    private String prepareFieldValue(String fieldName, String fieldValue, String itemType) {
        if (columnFamily.get(itemType).get(fieldName).equals("text") || columnFamily.get(itemType).get(fieldName).equals("timestamp")) {
            return "'" + fieldValue + "'";
        } else {
            return fieldValue;
        }
    }

    /**
     * Value from cassandra result set using field name.
     *
     * @param row       Cassandra Row
     * @param fieldName Column name
     * @return value from cassandra
     */
    public String getFieldValueFromResultset(Row row, String fieldName) {
        try {
            switch (columnFamily.get(itemApiImpl.getItemType()).get(fieldName)) {
                case ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT:
                    return row.getString(fieldName);
                case ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TIMESTAMP:
                    return String.valueOf(row.getTimestamp(fieldName));
                case ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT:
                    return String.valueOf(row.getInt(fieldName));
                case ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT:
                    return String.valueOf(row.getLong(fieldName));
                case ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_FLOAT:
                    return String.valueOf(row.getFloat(fieldName));
                case ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_DOUBLE:
                    return String.valueOf(row.getDouble(fieldName));
                case ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_LIST_INT:
                    List<Integer> Ids = row.getList(fieldName, Integer.class);
                    return Ids.toString();
                default:
                    return row.getString(fieldName);
            }
        } catch (Exception e) {
            System.out.println("Exception occurred - getFieldValueFromResultset() " + e.getMessage());
        }
        return "";
    }


    /**
     * Returns a String of ItemTypes specified in core.properties
     *
     * @return String: consisting comma separated ItemTypes.
     */
    public String getItemTypes() {
        return itemTypes;
    }

    /**
     * Returns a String of RecordTypes specified in core.properties
     *
     * @return String: consisting comma separated RecordTypes.
     */
    public String getRecordTypes() {
        return recordTypes;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    /**
     * Get the item or records export database casandra table name based on item type
     *
     * @param itemType Item type i.e Pdmarticle/Mamfile/Workflow
     * @return Item/Record table name of export database
     */
    public String getTableName(String itemType) {
        return prefixExport + "_" + itemType.toLowerCase();
    }

    /**
     * Get the sub item export database casandra table name based on item type and sub item type.
     *
     * @param itemType    Item type i.e Pdmarticle/Mamfile/Workflow
     * @param subItemType Sub Item type i.e configuration/mapping/reference/subtable
     * @return Sub item table name of export database
     */
    public String getTableName(String itemType, String subItemType) {
        String sItemType = itemType.toLowerCase();
        if (subItemType.equals(sSuffixReference)) {
            return prefixExport + "_" + sSuffixReference;
        } else if (sItemType.contains(itemTypeSuffix)) {
            sItemType = sItemType.substring(0, sItemType.length() - 9);
        }
        return prefixExport + "_" + sItemType + "_" + subItemType.toLowerCase();
    }

    private String getCommaSeparatedIds(List list) {
        String listString = list.toString();
        return listString.substring(1, listString.length() - 1);
    }
}