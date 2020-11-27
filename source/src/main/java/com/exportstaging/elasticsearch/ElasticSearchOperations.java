package com.exportstaging.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.connectors.idbconnector.IntermediateDAO;
import com.exportstaging.domain.ItemMessage;
import com.exportstaging.domain.OperationMessage;
import com.exportstaging.elasticsearch.clientbuilder.ClientBuilder;
import com.exportstaging.elasticsearch.querybuilder.ElasticIndexQueryBuilder;
import com.exportstaging.elasticsearch.queryexecutor.ElasticSearchQueryExecutor;
import com.exportstaging.elasticsearch.threadhandler.ElasticMainIndexRequestHandler;
import com.exportstaging.elasticsearch.utils.ElasticsearchUtils;
import com.exportstaging.subscribers.ElasticSubscriber;
import com.exportstaging.utilities.Html2Text;

import org.apache.commons.lang3.StringUtils;


@Component
public class ElasticSearchOperations
{


  ///// CONSTANTS //////////////////////////////////////////////////////////////////////////////////////////////////////


  /*
       Constants for Analyzer Information
     */
  private static final String EXPORT_PRIMARY_ANALYZER = "ExportPrimaryAnalyzer";
  private static final String EXPORT_RAW_ANALYZER     = "ExportRawAnalyzer";

  /*
     Constants for message Field
   */
  private static final String FIELD_LANGUAGEID  = "LanguageID";
  private static final String FIELD_ID          = "ID";
  private static final String FIELD_ATTRIBUTEID = "AttributeID";
  private static final String FIELD_ITEMID      = "ItemID";
  private static final String FIELD_CONTENT     = "_Content";
  private static final String FIELD_CONTENT_RAW = "_ContentRaw";

  private static final String CONSTANT_SUBITEMID    = "_SubitemID";
  private static final String CONSTANT_SUBTABLEIDS  = "_SubtableIDs";
  private static final String CONSTANT_REFERENCEIDS = "_ReferenceIDs";
  private static final String CONSTANT_ITEMTABLEID  = "ItemTableID";
  private static final String CONSTANT_REFERENCE    = "Reference";
  private static final String CONSTANT_SUBTABLE     = "Subtable";
  private static final String CONSTANT_ANALYZER     = "analyzer";
  private static final String CONSTANT_NORMALIZER   = "normalizer";

  /*
    Constants for elastic data type
   */
  private static final String CONSTANT_TYPE    = "type";
  private static final String CONSTANT_TEXT    = "text";
  private static final String CONSTANT_KEYWORD = "keyword";
  private static final String CONSTANT_STRING  = "string";
  private static final String CONSTANT_NUMERIC = "numeric";
  private static final String CONSTANT_DOUBLE  = "double";
  private static final String CONSTANT_DATE    = "date";

  /*
    Constants for elastic param and properties
   */
  private static final String CONSTANT_FILTER                   = "filter";
  private static final String CONSTANT_TOKENIZER                = "tokenizer";
  private static final String CONSTANT_FIELDS                   = "fields";
  private static final String CONSTANT_RAW                      = "raw";
  private static final String CONSTANT_CHAR_FILTER              = "char_filter";
  private static final String CONSTANT_IGNORE_MALFORMED         = "ignore_malformed";
  private static final String CONSTANT_MAPPING                  = "mapping";
  private static final String CONSTANT_MATCH                    = "match";
  private static final String CONSTANT_FIELD                    = "_field";
  private static final String CONSTANT_FORMAT                   = "format";
  private static final String CONSTANT_DYNAMIC_TEMPLATES        = "dynamic_templates";
  private static final String CONSTANT_IGNORE_ABOVE             = "ignore_above";
  private static final String CONSTANT_IGNORE_AVOVE_VALUE       = "10922";
  private static final String CONSTANT_STORE                    = "store";
  private static final int    CONSTANT_DEFAULT_MAPPING_FIELD    = 1000;
  private static final int    CONSTANT_DEFAULT_POOL_SIZE        = 10;
  private static final int    CONSTANT_DEFAULT_SEARCH_POOL_SIZE = 10;

  private static final int SCROLL_TIMEOUT = 60000;
  private static final int FETCH_SIZE     = 10000;

  private static final String VALID_DATE_FORMATS =
    "date_optional_time||epoch_millis||yyyy/MM/dd HH:mm:ss||" + "yyyy" + "/MM/dd HH:mm||yyyy/MM/dd||dd/MM/yyyy " +
    "HH:mm:ss||dd/MM/yyyy " + "HH:mm||dd/MM/yyyy||yyyy-MM-dd HH:mm:ss||" + "yyyy-MM-dd " + "HH:mm||yyyy-MM-dd||dd-MM" + "-yyyy HH:mm:ss||dd-MM-yyyy " + "HH:mm||dd-MM-yyyy||yyyy.MM.dd HH:mm:ss||" + "yyyy.MM.dd " + "HH:mm||yyyy.MM" + ".dd||dd.MM.yyyy HH:mm:ss||dd.MM.yyyy HH:mm||dd.MM" + ".yyyy||MM/dd/yy";

  ///// INSTANCE VARS //////////////////////////////////////////////////////////////////////////////////////////////////


  protected final Logger logger = LogManager.getLogger("exportstaging");

  @Value("${elasticsearch.table}")
  private String itemTypeName;
  @Value("${elasticsearch.table.filecontent}")
  private String fileContentTypeName;
  @Value("${elasticsearch.index.prefix}")
  private String elasticIndexPrefix;
  @Value("${json.key.item}")
  private String jsonKeyItem;
  @Value("${elasticsearch.index.shards.count}")
  private int    elasticShardsCount;
  @Value("${elasticsearch.replicationfactor}")
  private int    elasticReplicasCount;
  @Value("${activemq.subscriber.elastic}")
  private String elasticSubscriberName;
  @Value("${elasticsearch.content.length}")
  private int    elasticFileContentLength;
  @Value("${data.json.folder.headers}")
  private String headerFolder;
  @Value("${export.data.folder.root}")
  private String rootFolder;
  @Value("${cassandra.prefix.export}")
  private String prefixExport;
  @Value("${data.json.file.suffix.headers}")
  private String suffixHeaders;
  @Value("${elasticsearch.connection.delay}")
  private int    retryDelay;
  @Value("${mysql.type.item}")
  private String typeItem;
  @Value("${mysql.type.filecontent}")
  private String typeFileContent;
  @Value("${export.attribute.data.length}")
  private int    attributeDataLength;
  @Value("${elasticsearch.query.batchsize}")
  private int    elasticQueryBatchSize;
  @Value("${core.project.name}")
  private String projectName;
  @Value("${elasticsearch.index.max_result_window}")
  private long   indexMaxResultWindow;
  @Value("${elasticsearch.index.refresh.interval}")
  private String indexRefreshInterval;
  @Value("${elasticsearch.searchable.reindex.sleep}")
  private int    searchableReindexSleep;

  @Autowired
  private   ElasticMainIndexRequestHandler requestHandler;
  @Autowired
  private   ClientBuilder                  clientBuilder;
  @Autowired
  protected IntermediateDAO                intermediateDAO;
  @Autowired
  private   ElasticOperationHandler        handler;
  @Autowired
  protected ElasticSearchQueryExecutor     elasticQueryExecutor;
  @Autowired
  private   ElasticIndexQueryBuilder       elasticIndexQueryBuilder;
  @Autowired
  private   ElasticSubscriber              elasticSubscriber;

  private String          loggerMessage;
  private ExecutorService executor, searchIndexExecutor;
  private JSONParser jsonParser = new JSONParser();
  private Client     client     = null;
  private String[]   mappingNumericFields, mappingDateFields, mappingStringMultiFields;
  private Map<String, HashMap<String, HashMap<String, ArrayList<Object>>>> referenceMap = new HashMap<>();


  ElasticSearchOperations()
  {
  }

  ///// PUBLIC METHODS /////////////////////////////////////////////////////////////////////////////////////////////////


  public boolean removeIndex(String itemType, String subscriberName)
  {
    return handler.deleteIndices(itemType, subscriberName);
  }


  /**
   * Method will check the index and type exists or not for provided item type
   *
   * @param itemType  item type could be anyone from configured item types
   * @param indexName Name of the index to be validate
   *
   * @return will return true if index is exists otherwise false
   */
  public boolean validateIndex(String itemType, String indexName)
  {
    return handler.isIndexExisting(itemType, indexName) && isTypeExisting(indexName, ((itemType.equalsIgnoreCase(
      ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT))
                                                                                      ? fileContentTypeName
                                                                                      : itemTypeName));
  }


  /**
   * Method will be responsible to create elastic indexes if its not exists
   *
   * @param itemType  item type could be anyone from configured item types
   * @param indexName Name of the index to be created
   * @param dataModel object of model that is having information of headers
   */
  public void initializeIndex(String itemType, String indexName, JSONObject dataModel, String subscriberName)
    throws CannotCreateConnectionException
  {
    boolean isExisting;
    try {
      isExisting = handler.isIndexExisting(itemType, indexName);
      if (!isExisting) {
        ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
        JSONObject jsonObject = (JSONObject) dataModel.get(ExportMiscellaneousUtils.ES_FIELDS_STANDARD);
        this.setStandardAttributeArrays(jsonObject);
        int defaultFieldLimit = getDefaultFieldLimit(dataModel);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
          getIndexSettings(defaultFieldLimit));
        CreateIndexResponse createIndexResponse = clientBuilder.getIndicesAdminClient(subscriberName)
                                                               .create(createIndexRequest, RequestOptions.DEFAULT);
        if (createIndexResponse.isAcknowledged()) {
          initializeType(itemType, indexName);
          loggerMessage = "Index created: " + indexName;
          System.out.println(loggerMessage);
          logger.info("[" + elasticSubscriberName + "]" + loggerMessage);
        }
        else {
          loggerMessage = "Index " + indexName + " creation failed";
          System.err.println(loggerMessage);
          logger.error("[" + elasticSubscriberName + "]" + loggerMessage);
        }
      }
      else {
        loggerMessage = "Index " + indexName + " Exists";
        System.out.println(loggerMessage);
        logger.info("[" + elasticSubscriberName + "]" + loggerMessage);
        initializeType(itemType, indexName);
      }
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      initializeIndex(itemType, indexName, dataModel, subscriberName);
    } catch (java.lang.IllegalStateException e) {
      if (e.getCause() instanceof InterruptedException) {
        if (e.getMessage().equalsIgnoreCase("Future got interrupted")) {
          Thread.currentThread().interrupt();
        }
      }
    } catch (CannotCreateConnectionException e) {
      //Note: throw e because otherwise it will be caught in below Exception block
      throw e;
    } catch (Exception e) {
      handler.logError("Exception while creating index.", e);
    } finally {
      ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
    }
  }


  /**
   * Method will add filecontent document in elastic search
   *
   * @param message message containing file content that need to be add in elastic search
   * @param type    name of type
   * @param id      item id(currently it is mamfile id)
   *
   * @return if document added successfully in elastic it will return true otherwise false
   */
  public boolean addDocument(String message, String type, String id, String subscriberName)
  {
    String indexName = null;
    try {
      JSONObject jsonString = getJSONString(message, type);
      if (jsonString != null) {
        indexName = handler.getElasticIndexName(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT);
        IndexRequest indexRequest = new IndexRequest(indexName, handler.getElasticTypeName(type), id);
        indexRequest.source(jsonString);
        clientBuilder.getClient(subscriberName).index(indexRequest, RequestOptions.DEFAULT);
        return true;
      }
    } catch (IndexNotFoundException Exception) {
      try {
        elasticSubscriber.validateAndCreateIndex(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT, indexName);
        return addDocument(message, type, id, subscriberName);
      } catch (Exception e) {
        logger.info("Exception while creating index [" + indexName + "] in document insertion");
      }
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed." + " Retrying to perform operation", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return addDocument(message, type, id, subscriberName);
    } catch (java.lang.IllegalStateException e) {
      if (e.getCause() instanceof InterruptedException) {
        if (e.getMessage().equalsIgnoreCase("Future got interrupted")) {
          Thread.currentThread().interrupt();
        }
      }
    } catch (Exception e) {
      if (increaseMappingFieldsLimit(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT, e.getMessage())) {
        return addDocument(message, type, id, subscriberName);
      }
      handler.logError("Exception while adding document.", e);
    }
    return false;
  }


  /**
   * Method will add document for all the item types except mam file content
   *
   * @param message  message containing file content that need to be add in elastic search
   * @param itemType item type could be anyone from configured item types
   * @param type     name of type
   *
   * @return if document added successfully in elastic it will return true otherwise false
   */
  public boolean addDocumentForAllLanguages(ItemMessage message, String itemType, String type, String subscriberName)
  {
    JSONArray jsonItem = null;
    if (type.equals(typeItem)) {
      if (subscriberName.equalsIgnoreCase(elasticSubscriberName)) {
        jsonItem = getElasticSearchItem(message, itemType);
      }
      else {
        //Searchable index json preparation
        jsonItem = getElasticSearchItemForSearchIndex(message, itemType);
      }
    }
    else if (type.equals(typeFileContent) && subscriberName.equalsIgnoreCase(elasticSubscriberName)) {
      jsonItem = getElasticSearchFileContent(message.getRawMessage());
    }
    if (jsonItem == null) {
      return false;
    }
    return processBulkRequest(itemType, type, jsonItem, subscriberName);
  }


  /**
   * Method will be responsible for deletion of filecontent from elastic search
   *
   * @param ids  item id(currently it is mamfile id)
   * @param type name of type
   *
   * @return if document is deleted successfully from elastic it will return true otherwise false
   */
  public int deleteDocument(List<String> ids, String type, String subscriberName)
  {
    try {
      if (ids.size() != 0) {
        BulkRequest bulkRequest = new BulkRequest();
        String      indexName   = handler.getElasticIndexName(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT);
        for (String id : ids) {
          DeleteRequest deleteRequest = new DeleteRequest(indexName, type.toLowerCase(), id);
          bulkRequest.add(deleteRequest);
        }

        BulkResponse bulkResponse = clientBuilder.getClient(subscriberName).bulk(bulkRequest, RequestOptions.DEFAULT);

        if (bulkResponse.hasFailures()) {
          int    failedMessages = 0;
          String failedMessage  = bulkResponse.buildFailureMessage();
          if (failedMessage != null && StringUtils.isNotEmpty(failedMessage)) {
            if (elasticQueryExecutor.validateForIndexNotFound(failedMessage)) {
              logger.info(
                "[ElasticOperations] " + "Unable to delete the document, as index [" + indexName + "] is not existing");
            }
            else {
              failedMessages++;
            }
          }
          bulkFailureDebugLog(bulkResponse);
          return (failedMessages > 0)
                 ? 0
                 : 1;
        }
      }
      else {
        return -1;
      }
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return deleteDocument(ids, type, subscriberName);
    } catch (java.lang.IllegalStateException e) {
      if (e.getCause() instanceof InterruptedException) {
        if (e.getMessage().equalsIgnoreCase("Future got interrupted")) {
          Thread.currentThread().interrupt();
        }
      }
    } catch (Exception e) {
      handler.logError("Exception while deleting document.", e);
    }
    return 1;
  }


  /**
   * method will delete the document from all the exported languages
   *
   * @param ids            item id(currently it is mamfile id)
   * @param itemType       type of an item
   * @param subscriberName name of the subscriber like ElasticSubscriber or SearchableElasticSubscriber
   *
   * @return if document is deleted successfully from elastic it will return true otherwise false
   */
  public int deleteDocumentForAllLanguages(List<String> ids, String itemType, String subscriberName)
  {
    try {
      if (ids.size() != 0) {
        String        indexName;
        DeleteRequest deleteRequest;
        List<String>  languageIds = intermediateDAO.getLanguageIds();

        BulkRequest bulkRequest = new BulkRequest();

        for (String languageId : languageIds) {
          if (handler.getLanguagePerIndexFlag()) {
            if (subscriberName.equalsIgnoreCase(elasticSubscriberName)) {
              indexName = handler.getElasticIndexName(itemType, languageId);
            }
            else {
              indexName = handler.getSearchableIndexName(itemType, languageId);
            }
          }
          else {
            indexName = handler.getElasticIndexName(itemType);
          }

          for (String element : ids) {
            element = (handler.getLanguagePerIndexFlag()
                       ? element
                       : element + "_" + languageId);

            deleteRequest = new DeleteRequest(indexName, itemTypeName, element);
            bulkRequest.add(deleteRequest);
          }
        }

        BulkResponse bulkResponse = clientBuilder.getClient(subscriberName).bulk(bulkRequest, RequestOptions.DEFAULT);

        if (bulkResponse.hasFailures()) {
          int failedMessages = 0;
          logger.warn(bulkResponse.buildFailureMessage());
          BulkItemResponse bulkItemResponse[] = bulkResponse.getItems();
          for (BulkItemResponse bulkItemResponseTemp : bulkItemResponse) {
            String failedMessage = bulkItemResponseTemp.getFailureMessage();
            if (failedMessage != null && StringUtils.isNotEmpty(failedMessage)) {
              if (elasticQueryExecutor.validateForIndexNotFound(failedMessage)) {
                logger.info(
                  "[ElasticOperations] " + "Unable to delete the documents, as index [" + bulkItemResponseTemp.getIndex() + "] is not existing");
              }
              else {
                failedMessages++;
              }
            }
            logger.warn(bulkItemResponseTemp.getFailureMessage());
          }
          return (failedMessages > 0)
                 ? 0
                 : 1;
        }
      }
      else {
        return -1;
      }
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      logger.error(
        "[" + elasticSubscriberName + "] Unable to connect to ElasticSearch or Shard not available ." + " Retrying " + "to" + " " + "perform operation" + noHostOrShard
          .getMessage());
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return deleteDocumentForAllLanguages(ids, itemType, subscriberName);
    } catch (Exception e) {
      logger.error("[ElasticOperations] Exception:" + e.getMessage());
    }
    return 1;
  }


  /**
   * Updates items affected due to certain events
   *
   * @param messageData Information to be updated
   * @param itemType    Affected Item type
   *
   * @return status of update
   */
  public int updateAffectedItem(List<String> messageData, String itemType, String subscriberName)
  {
    List<String> affectedIds;
    String       deletedField      = null, deletedId = null, affectedItemType = null;
    int          updateStatus      = 1;
    List<String> affectedTypeArray = new ArrayList<>();
    if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_WORKFLOW)) {
      affectedItemType = messageData.get(1).toLowerCase();
      deletedField = messageData.get(2);
      deletedId = messageData.get(3);
      if (!"".equals(affectedItemType)) {
        String firstChar = String.valueOf(affectedItemType.charAt(0));
        affectedItemType = affectedItemType.replaceFirst(firstChar, firstChar.toUpperCase());
        affectedTypeArray.add(affectedItemType);
      }
    }
    else if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE)) {
      affectedTypeArray = ExportMiscellaneousUtils.getCoreItemTypes();
      deletedField = itemType + "ID";
      deletedId = messageData.get(0);
    }
    List<String> indices = new ArrayList<>();
    for (String type : affectedTypeArray) {
      if (ExportMiscellaneousUtils.getConfiguredTypes().contains(type)) {
        if (handler.getLanguagePerIndexFlag()) {
          for (String languageId : intermediateDAO.getLanguageIds()) {
            if (elasticSubscriberName.equalsIgnoreCase(subscriberName)) {
              indices.add(handler.getElasticIndexName(type, languageId));
            }
            else {
              indices.add(handler.getSearchableIndexName(type, languageId));
            }
          }
        }
        else {
          indices.add(handler.getElasticIndexName(type));
        }
      }
    }
    for (String indexName : indices) {
      affectedIds = getAffectedIds(indexName, deletedField, deletedId, subscriberName);
      if (affectedIds.size() == 0) {
        logger.info(
          "[" + subscriberName + "] : No affected items founds in " + indexName + " index to update on deletion of " + deletedField + ":" + deletedId);
        continue;
      }
      if (updateAffectedItemData(affectedIds, indexName, itemType, affectedItemType, subscriberName) == 0) {
        updateStatus = 0;
      }
      else {
        String logMessage =
          "[" + subscriberName + "] : Affected items data updated in " + indexName + " index on " + "deletion of " + deletedField + ":" + deletedId + " and affected document";
        logger.info(logMessage + " count in all languages: " + affectedIds.size());
        logger.debug(logMessage + "(s):" + affectedIds);
      }
    }
    return updateStatus;
  }


  /**
   * Prepare thread pool having pool size 10
   * This pool will be used to insert data in elastic search using different threads
   * Using the pool we can achieving multiple co-worker in elastic search
   */
  public void prepareThreadPool()
  {
    executor = Executors.newFixedThreadPool(CONSTANT_DEFAULT_POOL_SIZE);
    searchIndexExecutor = Executors.newFixedThreadPool(CONSTANT_DEFAULT_SEARCH_POOL_SIZE);
  }


  /**
   * Removed indices from elasticsearch based on passed item type and language ids
   *
   * @param itemTypes   Item type for which have to remove index.
   * @param languageIds Language ids for which have to remove index.
   *
   * @return true if successfully removed indices otherwise false
   */
  public boolean removeIndices(List<String> itemTypes, List<String> languageIds, String subscriberName)
  {
    return elasticIndexQueryBuilder.prepareAndRemoveIndices(itemTypes, languageIds, subscriberName);
  }


  ///// PRIVATE METHODS ////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Bulk operation for provided object will be done here
   *
   * @param itemType String type of object
   * @param type     String type of operation
   * @param jsonItem JSONArray Complete information about the object
   *
   * @return true on successful insertion otherwise false
   */
  private boolean processBulkRequest(String itemType, String type, JSONArray jsonItem, String subscriberName)
  {
    IndexRequest indexRequest;
    boolean      requestStatus = true;
    try {
      BulkRequest bulkRequest = new BulkRequest();
      String      elasticIndexName;
      String      documentId  = null;
      String      languageId;
      for (Object object : jsonItem) {
        JSONObject jsonObject = (JSONObject) object;
        languageId = jsonObject.get(ElasticsearchUtils.ES_FIELD_LANGUAGEID).toString();

        if (handler.getLanguagePerIndexFlag()) {
          if (subscriberName.equalsIgnoreCase(elasticSubscriberName)) {
            elasticIndexName = handler.getElasticIndexName(itemType, languageId);
          }
          else {
            elasticIndexName = handler.getSearchableIndexName(itemType, languageId);
          }
          documentId = jsonObject.get(ElasticsearchUtils.ES_FIELD_ID).toString();
        }
        else {
          elasticIndexName = handler.getElasticIndexName(itemType);
          documentId = jsonObject.get(FIELD_ID) + "_" + languageId;
        }


        indexRequest = new IndexRequest(elasticIndexName, handler.getElasticTypeName(type), documentId);
        indexRequest.source(jsonObject);
        bulkRequest.add(indexRequest);

        // Checks if languages more than elasticQueryBatchSize from elastic properties then request processed in batch
        if (bulkRequest.numberOfActions() > (elasticQueryBatchSize - 1)) {
          requestStatus = requestStatus && requestHandler.executeBulkRequest(bulkRequest);
          bulkRequest = new BulkRequest();
        }
      }

      if (bulkRequest.numberOfActions() > 0) {
        requestStatus = requestHandler.executeBulkRequest(bulkRequest);
      }
    } catch (IndexNotFoundException exception) {
      List<String> indexNames = exception.getHeader("indexNames");
      for (String indexName : indexNames) {
        try {
          elasticSubscriber.validateAndCreateIndex(itemType, indexName);
        } catch (Exception e) {
          logger.info("Exception while creating index [" + indexName + "] in document insertion");
        }
      }
      requestStatus = processBulkRequest(itemType, type, jsonItem, subscriberName);
    } catch (Exception e) {
      requestStatus = false;
      handler.logError("Exception while creating update query.", e);
    }

    return requestStatus;
  }


  /*private void processSearchableIndex(JSONArray jsonItem, String itemType, String type, boolean requestStatus)
  {
    if (handler.getLanguagePerIndexFlag()) {
      searchableIndexHandler.setSearchableIndexDetails(jsonItem, itemType, type);
      searchIndexExecutor.submit(searchableIndexHandler);
    }
  }*/


  /**
   * from the submitted thread we will get the result
   * true on successful otherwise false
   *
   * @param future Future object for which thread execution is done
   *
   * @return true on successful
   */
  private boolean getStatus(Future future)
  {
    boolean status = true;
    try {
      status = (boolean) future.get();
    } catch (InterruptedException e) {
      status = false;
    } catch (ExecutionException e) {
      status = false;
    }

    return status;
  }


  private int getDefaultFieldLimit(JSONObject headerModel)
  {
    int headerSize;
    if (headerModel == null) {
      headerSize = CONSTANT_DEFAULT_MAPPING_FIELD;
    }
    else {
      if (headerModel.size() != 0) {
        JSONObject standardFieldObject = (JSONObject) headerModel.get(ExportMiscellaneousUtils.ES_FIELDS_STANDARD);
        JSONObject customFieldObject   = (JSONObject) headerModel.get(ExportMiscellaneousUtils.ES_FIELDS_CUSTOM);
        if (customFieldObject != null) {
          standardFieldObject.putAll(customFieldObject);
        }
        headerSize = standardFieldObject.size();
      }
      else {
        headerSize = CONSTANT_DEFAULT_MAPPING_FIELD;
      }
    }
    return headerSize + (20 * headerSize / 100);
  }


  /**
   * Method will be responsible to initialize the dynamic template
   *
   * @param itemType  String item type could be anyone from configured item types
   * @param indexName String name of an index
   */
  private void initializeType(String itemType, String indexName)
  {
    itemType = itemType.toLowerCase();
    String          typeName = itemTypeName;
    XContentBuilder mappings;
    if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT)) {
      mappings = createFileContentMapping();
      typeName = fileContentTypeName;
    }
    else {
      mappings = createMappings(indexName);
    }
    createType(indexName, typeName, mappings);
  }


  /**
   * Method will be responsible to create type according to provided item type
   *
   * @param indexName name of an index
   * @param typeName  name of type
   * @param mappings  mapping details for Item or FileContent
   */
  private void createType(String indexName, String typeName, XContentBuilder mappings)
  {
    try {
      if (!isTypeExisting(indexName, typeName)) {
        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        putMappingRequest.type(typeName);
        putMappingRequest.source(mappings);
        AcknowledgedResponse putMappingResponse = clientBuilder.getIndicesAdminClient(handler.getSubscriberName())
                                                               .putMapping(putMappingRequest, RequestOptions.DEFAULT);
        if (putMappingResponse.isAcknowledged()) {
          loggerMessage = "Type Created: " + typeName + " [" + indexName + "]";
          System.out.println(loggerMessage);
          logger.info(loggerMessage);
        }
        else {
          loggerMessage = "Type " + typeName + " was not Created";
          System.out.println(loggerMessage);
          logger.error(loggerMessage);
        }
      }
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      createType(indexName, typeName, mappings);
    } catch (Exception e) {
      if (!isTypeExisting(indexName, typeName)) {
        handler.logError("Exception while creating type " + typeName + " for index " + indexName + ".", e);
      }
    }
  }


  private boolean isTypeExisting(String indexName, String typeName)
  {
    typeName = typeName.toLowerCase();
    boolean exists = false;
    try {
      exists = (clientBuilder.getAdmin(handler.getSubscriberName())
                             .performRequest(new Request("HEAD",
                                                         "/" + indexName + "/_mapping/" + typeName +
                                                         "?include_type_name=true"))
                             .getStatusLine()
                             .getStatusCode() == 200);
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return isTypeExisting(indexName, typeName);
    } catch (Exception e) {
      handler.logError("Exception while checking type " + typeName + " for indexName " + indexName, e);
    }
    return exists;
  }


  private JSONObject getJSONString(String message, String type)
  {
    JSONObject jsonMessage;
    JSONObject jsonFileContent;
    String     contentRaw = null;
    try {
      jsonMessage = (JSONObject) jsonParser.parse(message);
      if (jsonMessage != null) {
        String jsonString = jsonMessage.get(type).toString();
        jsonFileContent = (JSONObject) jsonParser.parse(jsonString);
        String fileContent = (String) jsonFileContent.get(FIELD_CONTENT);
        int    fileContentLen;
        if (fileContent != null) {
          if ((fileContentLen = fileContent.length()) > elasticFileContentLength) {
            contentRaw = fileContent.substring(0, elasticFileContentLength / 2) + " " + fileContent.substring(
              fileContentLen - elasticFileContentLength / 2);
          }
          else {
            contentRaw = fileContent;
          }
        }
        jsonFileContent.put(FIELD_CONTENT_RAW, contentRaw);
        jsonMessage.put(type, jsonFileContent);
      }
      return (JSONObject) (jsonMessage != null
                           ? jsonMessage.get(type)
                           : null);
    } catch (ParseException e) {
      handler.logError("Exception while getting json string for add document.", e);
    }
    return null;
  }


  private JSONArray getElasticSearchItem(ItemMessage message, String itemType)
  {
    Set<String> customHtmlFields = handler.getFields(itemType, ExportMiscellaneousUtils.CS_TYPE_HTML, ExportMiscellaneousUtils.ES_FIELDS_CUSTOM);  
  
    JSONArray                 itemArray         = new JSONArray();
    Map<String, List<String>> bigDataAttributes = message.getBigDataAttributes();
    referenceMap.clear();
    JSONObject references = getReferences(message.getParsedReference(), customHtmlFields);
    JSONObject subtables  = getSubtables(message.getParsedSubtable(), customHtmlFields);
    for (Object itemElement : message.getParsedItem()) {
      JSONObject   itemJSONElement = (JSONObject) itemElement;
      JSONObject removedNullFieldsItemJSONElement = processJson(itemJSONElement, customHtmlFields);
      String     languageID                       = (String) removedNullFieldsItemJSONElement.get(FIELD_LANGUAGEID);

      List<String> attributes      = bigDataAttributes.get(languageID);
      if (!attributes.isEmpty()) {
        bigDataHandler(removedNullFieldsItemJSONElement, attributes);
      }
      removedNullFieldsItemJSONElement.put(CONSTANT_REFERENCE, references.get(languageID));
      removedNullFieldsItemJSONElement.put(CONSTANT_SUBTABLE, subtables.get(languageID));
      itemArray.add(removedNullFieldsItemJSONElement);
    }
    return itemArray;
  }


  private JSONArray getElasticSearchItemForSearchIndex(ItemMessage message, String itemType)
  {
    Set<String> searchableHtmlFields = handler.getFields(itemType, ExportMiscellaneousUtils.CS_TYPE_HTML, ExportMiscellaneousUtils.ES_FIELDS_SEARCHABLE);  

    Set<String>               searchableFields  = handler.getSearchableItemHeaders(itemType);
    JSONArray                 itemArray         = new JSONArray();
    Map<String, List<String>> bigDataAttributes = message.getBigDataAttributes();
    referenceMap.clear();
    JSONObject references = getReferences(message.getParsedReference(), searchableHtmlFields);
    JSONObject subtables  = getSubtables(message.getParsedSubtable(), searchableHtmlFields);
    for (Object itemElement : message.getParsedItem()) {
      JSONObject itemJSONElement = (JSONObject) itemElement;
      itemJSONElement.keySet().retainAll(searchableFields);
      JSONObject   removedNullFieldsItemJSONElement = processJson(itemJSONElement, searchableHtmlFields);
      String       languageID                       = (String) removedNullFieldsItemJSONElement.get(FIELD_LANGUAGEID);
      List<String> attributes = bigDataAttributes.get(languageID);

      if (attributes != null && !attributes.isEmpty()) {
        bigDataHandler(removedNullFieldsItemJSONElement, attributes);
      }
      JSONObject referenceData = (JSONObject) references.get(languageID);
      if (referenceData != null) {
        referenceData.keySet().retainAll(intermediateDAO.getSearchableReferences(itemType));
        if (!referenceData.isEmpty()) {
          removedNullFieldsItemJSONElement.put(CONSTANT_REFERENCE, referenceData);
        }
      }

      JSONObject subtableData = (JSONObject) subtables.get(languageID);
      if (subtableData != null) {
        subtableData.keySet().retainAll(intermediateDAO.getSearchableSubtables(itemType));
        if (!subtableData.isEmpty()) {
          removedNullFieldsItemJSONElement.put(CONSTANT_SUBTABLE, subtableData);
        }
      }

      itemArray.add(removedNullFieldsItemJSONElement);
    }
    return itemArray;
  }


  private void bigDataHandler(JSONObject itemJsonObject, List<String> bigDataAttributes)
  {
    for (String attributeID : bigDataAttributes) {
      String jsonValue = (String) itemJsonObject.get(attributeID);
      if (jsonValue != null) {
        if (jsonValue.length() <= attributeDataLength) {
          continue;
        }
        int trimSize = attributeDataLength / 2;
        itemJsonObject.put(attributeID,
                           jsonValue.substring(0, trimSize) + jsonValue.substring(jsonValue.length() - trimSize));
      }
    }
  }


  private JSONArray getElasticSearchFileContent(String message)
  {
    JSONObject jsonMessage;
    try {
      jsonMessage = (JSONObject) jsonParser.parse(message);
      if (jsonMessage != null) {
        return (JSONArray) jsonMessage.get(ElasticsearchUtils.ES_CONSTANT_FILECONTENT);
      }
    } catch (ParseException e) {
      handler.logError("Exception while getting elastic search file content.", e);
    }
    return null;
  }


  private List<String> getAffectedIds(String indexName, String deletedField, String deletedId, String subscriberName)
  {
    List<String> affectedIds = new ArrayList<>();
    try {
      SearchRequest searchRequest = new SearchRequest(indexName).searchType(SearchType.DFS_QUERY_THEN_FETCH)
                                                                .scroll(new TimeValue(SCROLL_TIMEOUT));
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.query(QueryBuilders.matchQuery(deletedField, deletedId));
      searchSourceBuilder.fetchSource(false);
      searchSourceBuilder.size(FETCH_SIZE);
      searchRequest.source(searchSourceBuilder);

      SearchResponse searchResponse = client.search(searchRequest).get();

      String scrollId = searchResponse.getScrollId();

      do {
        for (SearchHit hit : searchResponse.getHits().getHits()) {
          affectedIds.add(hit.getId());
        }
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(new TimeValue(SCROLL_TIMEOUT));

        searchResponse = clientBuilder.getClient(subscriberName).scroll(scrollRequest, RequestOptions.DEFAULT);
        scrollId = searchResponse.getScrollId();
      }
      while (searchResponse.getHits().getHits().length != 0);
      clearScrollId(scrollId, subscriberName);

    } catch (IndexNotFoundException exception) {
      logger.info(
        "[ElasticOperations] " + "Unable to get affected IDs as index is not existing, Exception: " + exception.getMessage());
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return getAffectedIds(indexName, deletedField, deletedId, subscriberName);
    } catch (Exception e) {
      handler.logError("Exception while getting Affected Item Ids.", e);
    }
    return affectedIds;
  }


  private int updateAffectedItemData(
    List<String> affectedIds, String indexName, String itemType, String affectedItemType, String subscriberName
  )
  {
    int status = 1;
    try {
      BulkRequest bulkRequest = null;
      if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_WORKFLOW)) {
        bulkRequest = getUpdateQuery(indexName, affectedIds, subscriberName);
      }
      else if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE)) {
        bulkRequest = getDeleteQuery(indexName, affectedIds, subscriberName);
      }
      if (bulkRequest != null) {
        BulkResponse bulkResponse = clientBuilder.getClient(subscriberName).bulk(bulkRequest, RequestOptions.DEFAULT);

        if (bulkResponse.hasFailures()) {
          bulkFailureDebugLog(bulkResponse);
          return 0;
        }
        else if (handler.getLanguagePerIndexFlag() && itemType.equalsIgnoreCase(
          ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_WORKFLOW)) {
          //TODO put sleep to let complete main index delete operation for workflow/state
          Thread.sleep(searchableReindexSleep);
          JSONObject reindexJsonObject = new JSONObject();
          JSONObject dataJsonObject    = new JSONObject();
          dataJsonObject.put(ExportMiscellaneousUtils.CONSTANT_ITEM_TYPE, affectedItemType);
          dataJsonObject.put("AffectedItemIDs", affectedIds);
          reindexJsonObject.put(ExportMiscellaneousUtils.EXPORT_JSON_KEY_REINDEX, dataJsonObject);
          OperationMessage workflowStateDeletionMessage = new OperationMessage(reindexJsonObject.toString(), 1, 0,
                                                                               ExportMiscellaneousUtils.EXPORT_JSON_KEY_REINDEX);
          List<String> languageId = new ArrayList<>();
          languageId.add(indexName.substring(indexName.lastIndexOf("_") + 1));
          boolean reindexStatus = elasticSubscriber.operationTypeReindex(workflowStateDeletionMessage, languageId);
          if (!reindexStatus) {
            status = 0;
          }
        }
      }
    } catch (IndexNotFoundException exception) {
      logger.info(
        "[ElasticOperations] " + "Unable to update affected Items as index is not existing, Exception: " + exception.getMessage());
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return updateAffectedItemData(affectedIds, indexName, itemType, affectedItemType, subscriberName);
    } catch (Exception e) {
      handler.logError("Exception while updating affected items.", e);
    }
    return status;
  }


  private BulkRequest getUpdateQuery(String indexName, List<String> affectedIds, String subscriberName)
  {
    BulkRequest bulkRequest = null;
    try {
      bulkRequest = new BulkRequest();
      for (String affectedId : affectedIds) {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName);
        updateRequest.id(affectedId);
        updateRequest.doc(jsonBuilder().startObject()
                                       .field(ExportMiscellaneousUtils.EXPORT_FIELD_STATEID, "0")
                                       .field(ExportMiscellaneousUtils.EXPORT_FIELD_WORKFLOWID, "0")
                                       .endObject());
        bulkRequest.add(updateRequest);
      }
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return getUpdateQuery(indexName, affectedIds, subscriberName);
    } catch (Exception e) {
      handler.logError("Exception while creating update query.", e);
    }

    return bulkRequest;
  }


  private BulkRequest getDeleteQuery(String indexName, List<String> affectedIds, String subscriberName)
  {
    BulkRequest bulkRequest = null;
    try {
      bulkRequest = new BulkRequest();
      for (String affectedId : affectedIds) {
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index(indexName);
        deleteRequest.type(itemTypeName);
        deleteRequest.id(affectedId);
        bulkRequest.add(deleteRequest);
      }
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return getDeleteQuery(indexName, affectedIds, subscriberName);
    } catch (Exception e) {
      handler.logError("Exception while creating delete query.", e);
    }
    return bulkRequest;
  }


  private JSONObject getSubtables(JSONArray jsonArray, Set<String> htmlFields)
  {
    JSONObject                                                    jsonObject = new JSONObject();
    Map<String, Map<String, Map<String, Map<String, ArrayList>>>> itemMap    = new HashMap<>();
    JSONObject                                                    languageObject;
    JSONArray                                                     parentArray;
    String                                                        languageID;
    String                                                        attributeID;
    String                                                        itemID;
    String                                                        subItemID;

    if (jsonArray == null) {
      return jsonObject;
    }
    else {
      for (Object elementObject : jsonArray) {
        JSONObject elementJSONObject = (JSONObject) elementObject;
        JSONObject nullRemovalElementJsonObject = processJson(elementJSONObject, htmlFields);
        itemID = (String) elementJSONObject.get(FIELD_ITEMID);
        attributeID = (String) elementJSONObject.get(FIELD_ATTRIBUTEID);
        languageID = (String) elementJSONObject.get(FIELD_LANGUAGEID);
        subItemID = (String) elementJSONObject.get(CONSTANT_SUBITEMID);
        languageObject = jsonObject.get(languageID) == null
                         ? new JSONObject()
                         : (JSONObject) jsonObject.get(languageID);

        if (!itemID.equals(subItemID)) {
          if (itemMap.get(itemID) != null) {
            if (itemMap.get(itemID).get(languageID) != null) {
              if (itemMap.get(itemID).get(languageID).get(attributeID) != null) {
                if (itemMap.get(itemID).get(languageID).get(attributeID).get(subItemID) != null) {
                  itemMap.get(itemID).get(languageID).get(attributeID).get(subItemID).add(nullRemovalElementJsonObject);
                }
                else {
                  itemMap.get(itemID).get(languageID).get(attributeID).put(subItemID, new ArrayList());
                  itemMap.get(itemID).get(languageID).get(attributeID).get(subItemID).add(nullRemovalElementJsonObject);
                }
              }
              else {
                itemMap.get(itemID).get(languageID).put(attributeID, new HashMap<>());
                itemMap.get(itemID).get(languageID).get(attributeID).put(subItemID, new ArrayList());
                itemMap.get(itemID).get(languageID).get(attributeID).get(subItemID).add(nullRemovalElementJsonObject);
              }
            }
            else {
              itemMap.get(itemID).put(languageID, new HashMap<>());
              itemMap.get(itemID).get(languageID).put(attributeID, new HashMap<>());
              itemMap.get(itemID).get(languageID).get(attributeID).put(subItemID, new ArrayList());
              itemMap.get(itemID).get(languageID).get(attributeID).get(subItemID).add(nullRemovalElementJsonObject);
            }
          }
          else {
            itemMap.clear();
            itemMap.put(itemID, new HashMap<>());
            itemMap.get(itemID).put(languageID, new HashMap<>());
            itemMap.get(itemID).get(languageID).put(attributeID, new HashMap<>());
            itemMap.get(itemID).get(languageID).get(attributeID).put(subItemID, new ArrayList());
            itemMap.get(itemID).get(languageID).get(attributeID).get(subItemID).add(nullRemovalElementJsonObject);
          }
          handleNestedLevel(itemID, itemMap, nullRemovalElementJsonObject, languageID);
          jsonObject.put(languageID, languageObject);
        }
        else {
          if (languageObject != null) {
            parentArray = languageObject.get(attributeID) == null
                          ? new JSONArray()
                          : (JSONArray) languageObject.get(attributeID);
          }
          else {
            parentArray = new JSONArray();
            languageObject = new JSONObject();
          }
          itemID = (String) nullRemovalElementJsonObject.get(FIELD_ITEMID);
          if (itemMap.isEmpty()) {
            itemMap.put(itemID, new HashMap<>());
            itemMap.get(itemID).put(languageID, new HashMap<>());
            itemMap.get(itemID).get(languageID).put(attributeID, new HashMap<>());
            itemMap.get(itemID).get(languageID).get(attributeID).put(subItemID, new ArrayList());
            itemMap.get(itemID).get(languageID).get(attributeID).get(subItemID).add(nullRemovalElementJsonObject);
          }
          handleNestedLevel(itemID, itemMap, nullRemovalElementJsonObject, languageID);
          parentArray.add(nullRemovalElementJsonObject);
          languageObject.put(attributeID, parentArray);
        }
        jsonObject.put(languageID, languageObject);
      }
      return jsonObject;
    }
  }


  private void handleNestedLevel(
    String itemID,
    Map<String, Map<String, Map<String, Map<String, ArrayList>>>> itemMap,
    JSONObject elementJSONObject,
    String languageID
  )
  {
    if (itemMap.containsKey(itemID)) {
      String subtableID   = (String) elementJSONObject.get(CONSTANT_SUBTABLEIDS);
      String referenceIDs = (String) elementJSONObject.get(CONSTANT_REFERENCEIDS);
      String tableRowID   = elementJSONObject.get(CONSTANT_ITEMTABLEID).toString();

      JSONObject subtableObject = new JSONObject();
      if (subtableID != null) {
        appendSubtableData(subtableID, referenceIDs, languageID, tableRowID, elementJSONObject, subtableObject, itemMap,
                           itemID);
      }
      else {  // reference attribute is present but the subtable data is empty
        if (referenceIDs != null) {
          appendReferenceData(elementJSONObject, referenceIDs, new JSONObject(), languageID, tableRowID);
        }
      }
    }
  }


  private JSONObject appendSubtableData(
    String subtableID,
    String referenceIDs,
    String languageID,
    String tableRowID,
    JSONObject elementJSONObject,
    JSONObject subtableObject,
    Map<String, Map<String, Map<String, Map<String, ArrayList>>>> itemMap,
    String itemID
  )
  {
    String[] subTableIDs = subtableID.split(",");
    for (String tableID : subTableIDs) {
      JSONObject referenceDataObject = new JSONObject();
      if (referenceIDs != null) {
        elementJSONObject = appendReferenceData(elementJSONObject, referenceIDs, referenceDataObject, languageID,
                                                tableRowID);
      }
      subtableObject.put(tableID, itemMap.get(itemID).get(languageID).get(tableID).get(tableRowID));
      elementJSONObject.put(CONSTANT_SUBTABLE, subtableObject);
    }
    return elementJSONObject;
  }


  private JSONObject appendReferenceData(
    JSONObject elementJSONObject,
    String referenceIDs,
    JSONObject referenceDataObject,
    String languageID,
    String tableRowID
  )
  {
    String[] referenceIDList = referenceIDs.split(",");
    for (String refID : referenceIDList) {
      if (referenceMap.get(refID) != null) {
        referenceDataObject.put(refID, referenceMap.get(refID).get(languageID).get(tableRowID));
        elementJSONObject.put(CONSTANT_REFERENCE, referenceDataObject);
      }
    }
    return elementJSONObject;
  }


  private JSONObject getReferences(JSONArray jsonArray, Set<String> htmlFields)
  {
    JSONObject jsonObject = new JSONObject();

    JSONObject languageObject;
    JSONArray  elementArray;
    String     subItemID;

    for (Object elementObject : jsonArray) {
      JSONObject elementJSONObject = (JSONObject) elementObject;
      Object     nullRemovedElementObject = processJson(elementJSONObject, htmlFields);;
      String     attributeID       = (String) elementJSONObject.get(FIELD_ATTRIBUTEID);
      String     itemID            = (String) elementJSONObject.get(FIELD_ITEMID);
      String     languageID        = (String) elementJSONObject.get(FIELD_LANGUAGEID);
      subItemID = (String) elementJSONObject.get(CONSTANT_SUBITEMID);

      if (subItemID.equals(itemID)) {
        languageObject = (JSONObject) jsonObject.get(languageID);
        if (languageObject != null) {
          elementArray = (JSONArray) languageObject.get(attributeID);
          if (elementArray == null) {
            elementArray = new JSONArray();
          }
        }
        else {
          elementArray = new JSONArray();
          languageObject = new JSONObject();
        }
        elementArray.add(nullRemovedElementObject);
        languageObject.put(attributeID, elementArray);
        jsonObject.put(languageID, languageObject);
      }
      else {        //if reference attribute is part of a subtable
        if (referenceMap.get(attributeID) != null) {
          if (referenceMap.get(attributeID).get(languageID) != null) {
            if (referenceMap.get(attributeID).get(languageID).get(subItemID) != null) {
              referenceMap.get(attributeID).get(languageID).get(subItemID).add(nullRemovedElementObject);
            }
            else {
              referenceMap.get(attributeID).get(languageID).put(subItemID, new ArrayList<>());
              referenceMap.get(attributeID).get(languageID).get(subItemID).add(nullRemovedElementObject);
            }
          }
          else {
            referenceMap.get(attributeID).put(languageID, new HashMap<>());
            referenceMap.get(attributeID).get(languageID).put(subItemID, new ArrayList<>());
            referenceMap.get(attributeID).get(languageID).get(subItemID).add(nullRemovedElementObject);
          }
        }
        else {
          referenceMap.put(attributeID, new HashMap<>());
          referenceMap.get(attributeID).put(languageID, new HashMap<>());
          referenceMap.get(attributeID).get(languageID).put(subItemID, new ArrayList<>());
          referenceMap.get(attributeID).get(languageID).get(subItemID).add(nullRemovedElementObject);
        }
      }
    }
    return jsonObject;
  }


  private XContentBuilder getIndexSettings(int defaultFieldLimit)
  {
    XContentBuilder settings = null;
    try {
      settings = jsonBuilder().startObject()
                              .field(ExportMiscellaneousUtils.EXPORT_ELASTIC_SHARD_NUMBER, elasticShardsCount)
                              .field(ExportMiscellaneousUtils.EXPORT_ELASTIC_SETTING_FIELD_LIMIT, defaultFieldLimit)
                              .field(ExportMiscellaneousUtils.EXPORT_ELASTIC_REPLICA_NUMBER, elasticReplicasCount)
                              .field(ExportMiscellaneousUtils.EXPORT_ELASTIC_SETTING_MAX_RESULT_WINDOW,
                                     indexMaxResultWindow)
                              .field(ExportMiscellaneousUtils.EXPORT_ELASTIC_SETTING_INDEX_REFRESH_INTERVAL,
                                     indexRefreshInterval)
                              .startObject("analysis")
                              .startObject(CONSTANT_ANALYZER)
                              .startObject(EXPORT_PRIMARY_ANALYZER)
                              .field(CONSTANT_TYPE, "custom")
                              .field(CONSTANT_TOKENIZER, "whitespace")
                              .field(CONSTANT_FILTER, "lowercase")
                              .field(CONSTANT_CHAR_FILTER, "html_strip")
                              .endObject()
                              .endObject()
                              .startObject(CONSTANT_NORMALIZER)
                              .startObject(EXPORT_RAW_ANALYZER)
                              .field(CONSTANT_TYPE, "custom")
                              .field(CONSTANT_FILTER, "lowercase")
                              .startObject("char_filter")
                              .field("char_filter", "html_strip")
                              .endObject()
                              .endObject()
                              .endObject()
                              .endObject()
                              .endObject();
    } catch (IOException e) {
      handler.logError("Exception while getting index setting.", e);
    }
    return settings;
  }


  private XContentBuilder createMappings(String indexName)
  {
    String mappingFormattedValue = "*:FormattedValue";
    String mappingValue          = "*:Value";
    try {
      XContentBuilder mappings = jsonBuilder().startObject().startArray(CONSTANT_DYNAMIC_TEMPLATES);
      getNumericFieldsMapping(mappings, mappingNumericFields);
      getNumericFieldMapping(mappings, "ID");
      getDateFieldsMapping(mappings, mappingDateFields);
      getStringMultiFieldsMapping(mappings, mappingStringMultiFields);
      getStringFieldMapping(mappings, mappingFormattedValue);
      getValueFieldMapping(mappings, mappingValue);
      getDefaultNumericMapping(mappings, "*", "long");
      getDefaultNumericMapping(mappings, "*", "double");
      getDefaultDateMapping(mappings, "*");
      //getDefaultStringMultiFieldMapping(mappings, "*");
      mappings.endArray();

      if (indexName.contains(ElasticsearchUtils.ES_SEARCH_INDEX_SUFFIX)) {
        mappings.startObject("_source");
        mappings.field("enabled", "false");
        mappings.endObject();
      }
      mappings.endObject();
      return mappings;
    } catch (IOException e) {
      handler.logError("Exception while creating mappings.", e);
      return null;
    }
  }


  private XContentBuilder createFileContentMapping()
  {
    try {
      XContentBuilder mappings = jsonBuilder().startObject().startArray(CONSTANT_DYNAMIC_TEMPLATES);
      getNumericFieldMapping(mappings, FIELD_ID);
      getStringFieldMapping(mappings, FIELD_CONTENT_RAW);
      getStringFieldMappingOfPrimaryAnalyzer(mappings, FIELD_CONTENT);
      mappings.endArray();
      mappings.endObject();
      return mappings;
    } catch (IOException e) {
      handler.logError("Exception while creating file content mapping.", e);
      return null;
    }
  }


  private void getNumericFieldsMapping(XContentBuilder mappings, String[] aFields)
  {
    try {
      if (aFields == null || aFields.length == 0) {
        return;
      }

      for (String sField : aFields) {
        getNumericFieldMapping(mappings, sField);
      }
    } catch (Exception e) {
      handler.logError("Exception while getting numeric field mapping.", e);
    }
  }


  private void getDateFieldsMapping(XContentBuilder mappings, String[] aFields)
  {
    try {
      if (aFields == null || aFields.length == 0) {
        return;
      }
      for (String sField : aFields) {
        getDateFieldMapping(mappings, sField);
      }
    } catch (Exception e) {
      handler.logError("Exception while getting date fields mapping.", e);
    }
  }


  private void getStringMultiFieldsMapping(XContentBuilder mappings, String[] aFields)
  {
    try {
      if (aFields == null || aFields.length == 0) {
        return;
      }
      for (String sField : aFields) {
        getStringMultiFieldMapping(mappings, sField);
      }
    } catch (Exception e) {
      handler.logError("Exception while getting string multi fields mapping.", e);
    }
  }


  private void getDateFieldMapping(XContentBuilder mappings, String sField)
  {
    try {
      mappings.startObject()
              .startObject(getFieldName(sField))
              .field(CONSTANT_MATCH, sField)
              .startObject(CONSTANT_MAPPING)
              .field(CONSTANT_TYPE, CONSTANT_KEYWORD)
              .field(CONSTANT_NORMALIZER, EXPORT_RAW_ANALYZER);

      /**
       * Storing the fields for fast retrieval of these data.
       * LastChange => pdmarticle, pdmarticlestructure and mamfile
       * Changed    => user
       */
      if (sField.equalsIgnoreCase("LastChange") || sField.equalsIgnoreCase("Changed")) {
        mappings.field(CONSTANT_STORE, true);
      }

      mappings.startObject(CONSTANT_FIELDS)
              .startObject(CONSTANT_DATE)
              .field(CONSTANT_TYPE, CONSTANT_DATE)
              .field(CONSTANT_FORMAT, VALID_DATE_FORMATS)
              .field(CONSTANT_IGNORE_MALFORMED, true);

      /**
       * Storing the fields for fast retrieval of these data.
       * LastChange => pdmarticle, pdmarticlestructure and mamfile
       * Changed    => user
       */
      if (sField.equalsIgnoreCase("LastChange") || sField.equalsIgnoreCase("Changed")) {
        mappings.field(CONSTANT_STORE, true);
      }

      mappings.endObject().endObject().endObject().endObject().endObject();
    } catch (Exception e) {
      handler.logError("Exception while getting date field mapping.", e);
    }
  }


  private void getStringMultiFieldMapping(XContentBuilder mappings, String sField)
  {
    try {
      mappings.startObject()
              .startObject(getFieldName(sField))
              .field(CONSTANT_MATCH, sField)
              .startObject(CONSTANT_MAPPING)
              .field(CONSTANT_TYPE, CONSTANT_TEXT)
              .field(CONSTANT_ANALYZER, EXPORT_PRIMARY_ANALYZER)
              .startObject(CONSTANT_FIELDS)
              .startObject(CONSTANT_RAW)
              .field(CONSTANT_IGNORE_ABOVE, CONSTANT_IGNORE_AVOVE_VALUE)
              .field(CONSTANT_TYPE, CONSTANT_KEYWORD)
              .field(CONSTANT_NORMALIZER, EXPORT_RAW_ANALYZER)
              .endObject()
              .endObject()
              .endObject()
              .endObject()
              .endObject();
    } catch (Exception e) {
      handler.logError("Exception while getting string multi field mapping.", e);
    }
  }


  private void getNumericFieldMapping(XContentBuilder mappings, String sField)
  {
    try {
      mappings.startObject()
              .startObject(getFieldName(sField))
              .field(CONSTANT_MATCH, sField)
              .startObject(CONSTANT_MAPPING)
              .field(CONSTANT_TYPE, CONSTANT_DOUBLE)
              .field(CONSTANT_ANALYZER, EXPORT_PRIMARY_ANALYZER);

             /*Storing the fields for fast retrieval of these data.
             VersionNr => pdmarticle, pdmarticlestructure and mamfile*/
      if (sField.equalsIgnoreCase("VersionNr")) {
        mappings.field(CONSTANT_STORE, true);
      }

      mappings.endObject().endObject().endObject();
    } catch (Exception e) {
      handler.logError("Exception while getting numeric field mapping.", e);
    }
  }


  private void getDefaultStringMultiFieldMapping(XContentBuilder mappings, String sField)
  {
    try {
      mappings.startObject()
              .startObject(getFieldName(sField))
              .field("match_mapping_type", CONSTANT_STRING)
              .field(CONSTANT_MATCH, "*")
              .startObject(CONSTANT_MAPPING)
              .field(CONSTANT_TYPE, CONSTANT_TEXT)
              .field(CONSTANT_ANALYZER, EXPORT_PRIMARY_ANALYZER)
              .startObject(CONSTANT_FIELDS)
              .startObject(CONSTANT_RAW)
              .field(CONSTANT_TYPE, CONSTANT_KEYWORD)
              .field(CONSTANT_NORMALIZER, EXPORT_RAW_ANALYZER)
              .endObject()
              .endObject()
              .endObject()
              .endObject()
              .endObject();
    } catch (Exception e) {
      handler.logError("Exception while getting default string multi field mapping.", e);
    }
  }


  private void getDefaultDateMapping(XContentBuilder mappings, String sField)
  {
    try {
      mappings.startObject()
              .startObject(getFieldName(sField))
              .field("match_mapping_type", CONSTANT_DATE)
              .field(CONSTANT_MATCH, "*")
              .startObject(CONSTANT_MAPPING)
              .field(CONSTANT_TYPE, CONSTANT_KEYWORD)
              .field(CONSTANT_NORMALIZER, EXPORT_RAW_ANALYZER)
              .startObject(CONSTANT_FIELDS)
              .startObject(CONSTANT_DATE)
              .field(CONSTANT_TYPE, CONSTANT_DATE)
              .field(CONSTANT_FORMAT, VALID_DATE_FORMATS)
              .field(CONSTANT_IGNORE_MALFORMED, true)
              .endObject()
              .endObject()
              .endObject()
              .endObject()
              .endObject();
    } catch (Exception e) {
      handler.logError("Exception while getting default date field mapping.", e);
    }
  }


  private void getDefaultNumericMapping(XContentBuilder mappings, String sField, String matchMappingType)
  {
    try {
      mappings.startObject()
              .startObject(getFieldName(sField))
              .field("match_mapping_type", matchMappingType)
              .field(CONSTANT_MATCH, "*")
              .startObject(CONSTANT_MAPPING)
              .field(CONSTANT_TYPE, CONSTANT_DOUBLE)
              .field(CONSTANT_ANALYZER, EXPORT_PRIMARY_ANALYZER)
              .endObject()
              .endObject()
              .endObject();
    } catch (Exception e) {
      handler.logError("Exception while getting default numeric field mapping.", e);
    }
  }


  private void getStringFieldMapping(XContentBuilder mappings, String sField)
  {
    try {
      mappings.startObject()
              .startObject(getFieldName(sField))
              .field(CONSTANT_MATCH, sField)
              .startObject(CONSTANT_MAPPING)
              .field(CONSTANT_TYPE, CONSTANT_KEYWORD)
              .field(CONSTANT_IGNORE_ABOVE, CONSTANT_IGNORE_AVOVE_VALUE)
              .field(CONSTANT_NORMALIZER, EXPORT_RAW_ANALYZER)
              .endObject()
              .endObject()
              .endObject();
    } catch (Exception e) {
      handler.logError("Exception while getting string field mapping.", e);
    }
  }


  private void getStringFieldMappingOfPrimaryAnalyzer(XContentBuilder mappings, String sField)
  {
    try {
      mappings.startObject()
              .startObject(getFieldName(sField))
              .field(CONSTANT_MATCH, sField)
              .startObject(CONSTANT_MAPPING)
              .field(CONSTANT_TYPE, CONSTANT_TEXT)
              .field(CONSTANT_ANALYZER, EXPORT_PRIMARY_ANALYZER)
              .endObject()
              .endObject()
              .endObject();
    } catch (Exception e) {
      handler.logError("Exception while getting string field mapping of primary analyzer.", e);
    }
  }


  private void getValueFieldMapping(XContentBuilder mappings, String sField)
  {
    try {
      mappings.startObject()
              .startObject(getFieldName(sField))
              .field(CONSTANT_MATCH, sField)
              .startObject(CONSTANT_MAPPING)
              .field(CONSTANT_TYPE, CONSTANT_TEXT)
              .field(CONSTANT_ANALYZER, EXPORT_PRIMARY_ANALYZER)
              .startObject(CONSTANT_FIELDS)
              .startObject(CONSTANT_DATE)
              .field(CONSTANT_TYPE, CONSTANT_DATE)
              .field(CONSTANT_FORMAT, VALID_DATE_FORMATS)
              .field(CONSTANT_IGNORE_MALFORMED, true)
              .endObject()
              .startObject(CONSTANT_NUMERIC)
              .field(CONSTANT_TYPE, CONSTANT_DOUBLE)
              .field(CONSTANT_IGNORE_MALFORMED, true)
              .endObject()
              .startObject(CONSTANT_RAW)
              .field(CONSTANT_TYPE, CONSTANT_KEYWORD)
              .field(CONSTANT_IGNORE_ABOVE, CONSTANT_IGNORE_AVOVE_VALUE)
              .field(CONSTANT_NORMALIZER, EXPORT_RAW_ANALYZER)
              .endObject()
              .endObject()
              .endObject()
              .endObject()
              .endObject();
    } catch (Exception e) {
      handler.logError("Exception while getting value field mapping.", e);
    }
  }


  private String getFieldName(String sField)
  {
    String sFieldName = sField;
    int    startIndex = 0;
    if (sField.contains("*:")) {
      startIndex = 2;
    }
    else if (sField.contains("*")) {
      startIndex = 1;
    }
    if (startIndex > 0) {
      sFieldName = sFieldName.substring(startIndex, sFieldName.length());
    }
    sFieldName += CONSTANT_FIELD;
    return sFieldName.toLowerCase();

  }


 /* private List<String> getDocumentsByIDs(List<String> ids, String itemType)
  {
    List<String> results = new ArrayList<>();
    String       scrollId;
    try {
      QueryBuilder termsQueryBuilder = QueryBuilders.termsQuery(FIELD_ID, ids.toArray());

      SearchResponse searchResponse = clientBuilder.getClient()
                                                   .prepareSearch(handler.getElasticIndexName(itemType))
                                                   .setScroll(new TimeValue(60000))
                                                   .setQuery(termsQueryBuilder)
                                                   .setSize(10000)
                                                   .execute()
                                                   .actionGet();
      do {
        for (SearchHit hit : searchResponse.getHits().getHits()) {
          results.add(hit.getId());
        }
        searchResponse = clientBuilder.getClient()
                                      .prepareSearchScroll(searchResponse.getScrollId())
                                      .setScroll(new TimeValue(60000))
                                      .execute()
                                      .actionGet();
        scrollId = searchResponse.getScrollId();
      }
      while (searchResponse.getHits().getHits().length != 0);
      clearScrollId(scrollId, subscriberName);
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return getDocumentsByIDs(ids, itemType);
    } catch (Exception e) {
      handler.logError("Exception while fetching documents by ids.", e);
    }
    return results;
  }*/


  private void clearScrollId(String scrollId, String subscriberName) throws CannotCreateConnectionException, IOException
  {
    ClearScrollRequest request = new ClearScrollRequest();
    request.addScrollId(scrollId);
    clientBuilder.getClient(subscriberName).clearScroll(request, RequestOptions.DEFAULT);
  }


  private void setStandardAttributeArrays(JSONObject dataModel)
  {
    if (dataModel != null) {
      List<String> numericAttributes = new ArrayList<>();
      List<String> dateAttributes    = new ArrayList<>();
      List<String> stringAttributes  = new ArrayList<>();
      try {
        Set headerData = dataModel.keySet();
        for (Object key : headerData) {
          String field = (String) key;
          if (dataModel.get(field).equals(ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TEXT)) {
            stringAttributes.add(field);
          }
          else if (dataModel.get(field).equals(ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_INT) || dataModel.get(
            field).equals(ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_BIGINT) || dataModel.get(field)
                                                                                                 .equals(
                                                                                                   ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_FLOAT)) {
            numericAttributes.add(field);
          }
          else if (dataModel.get(field).equals(ExportMiscellaneousUtils.EXPORT_DATABASE_DATA_TYPE_TIMESTAMP)) {
            dateAttributes.add(field);
          }
        }
        mappingNumericFields = numericAttributes.toArray(new String[0]);
        mappingDateFields = dateAttributes.toArray(new String[0]);
        mappingStringMultiFields = stringAttributes.toArray(new String[0]);
      } catch (Exception exception) {
        handler.logError("Exception while setting standard attribute array.", exception);
      }
    }
  }


  /**
   * Default fields limit is 1000 for per index but if its exceeded then it would be set to new limit
   * New limit would be current limit + 50% of current limit
   *
   * @param itemType       type of an item
   * @param failureMessage message that we need to check before setting new limit
   */
  private boolean increaseMappingFieldsLimit(String itemType, String failureMessage)
  {
    if (validateForMappingFieldsLimit(failureMessage)) {
      String indexName = handler.getElasticIndexName(itemType);
      int currentMappingFieldsLimit = getIndexSettingValue(indexName,
                                                           ExportMiscellaneousUtils.EXPORT_ELASTIC_SETTING_FIELD_LIMIT);
      int newMappingFieldsLimit = (currentMappingFieldsLimit + (currentMappingFieldsLimit / 2));
      try {
        UpdateSettingsRequest request = new UpdateSettingsRequest(handler.getElasticIndexName(itemType));
        request.settings(
          Settings.builder().put(ExportMiscellaneousUtils.EXPORT_ELASTIC_SETTING_FIELD_LIMIT, newMappingFieldsLimit));
        clientBuilder.getIndicesAdminClient(handler.getSubscriberName()).putSettings(request, RequestOptions.DEFAULT);
      } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
        handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
        try {
          Thread.sleep(retryDelay);
        } catch (InterruptedException interrupt) {
          Thread.currentThread().interrupt();
        }
        return increaseMappingFieldsLimit(itemType, failureMessage);
      } catch (Exception e) {
        handler.logError("Exception while increasing field limit.", e);
      }
      logger.warn(
        "[" + elasticSubscriberName + "]: " + ExportMiscellaneousUtils.EXPORT_ELASTIC_FIELD_LIMIT + newMappingFieldsLimit);
      return true;
    }
    return false;
  }


  /**
   * Method will take setting name as an argument and provide its assign current value
   *
   * @param indexName   name of an index
   * @param settingName setting name for which value will be fetched, it would give only integer values
   *
   * @return value of a setting
   */
  private int getIndexSettingValue(String indexName, String settingName)
  {
    int currentMappingFieldLimit = 0;
    try {
      GetSettingsRequest request = new GetSettingsRequest().indices(indexName);
      GetSettingsResponse response = clientBuilder.getIndicesAdminClient(handler.getSubscriberName())
                                                  .getSettings(request, RequestOptions.DEFAULT);
      for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
        Settings settings = cursor.value;
        currentMappingFieldLimit = settings.getAsInt(settingName, ElasticsearchUtils.ES_DEFAULT_MAPPING_FIELD);
      }
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      handler.logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return getIndexSettingValue(indexName, settingName);
    } catch (Exception e) {
      handler.logError("Exception while retrieving settings value.", e);
    }

    return currentMappingFieldLimit;
  }


  /**
   * Method will validate the error whether its because of fields limit or not
   *
   * @param errorMessage error message
   *
   * @return will return true if its because of fields limit exceeded otherwise false
   */
  private boolean validateForMappingFieldsLimit(String errorMessage)
  {
    boolean status = false;
    if (errorMessage.contains(ExportMiscellaneousUtils.EXPORT_ELASTIC_FIELD_LIMIT_MESSAGE)) {
      status = true;
    }

    return status;
  }


  /**
   * Method will logging debug log for bulk failure operation
   *
   * @param bulkResponse {@link BulkResponse}
   */
  private void bulkFailureDebugLog(BulkResponse bulkResponse)
  {
    BulkItemResponse bulkItemResponse[] = bulkResponse.getItems();
    for (BulkItemResponse bulkItemResponseTemp : bulkItemResponse) {
      logger.debug(bulkItemResponseTemp.getFailureMessage());
    }
  }
  
  /**
   * Process given json for null and html fields
   * 
   * @param itemJSONElement json to process
   * @param htmlFields fields of html type
   * @return processed json object
   * @throws ExportStagingException
   */
  private JSONObject processJson(JSONObject itemJSONElement, Set<String> htmlFields) {
    JSONObject processedJson = new JSONObject(itemJSONElement);
    for (Iterator iterator = itemJSONElement.keySet().iterator(); iterator.hasNext(); ) {
      String key = (String) iterator.next();
      
      removeNull(processedJson, key);

      removeHtmlTags(htmlFields, processedJson, key);
    }
    return processedJson;
  }

  /**
   * Removes html tags for given key
   * 
   * @param htmlFields html fields from CS side
   * @param processedJson json to process
   * @param key key to process
   * @throws ExportStagingException
   */
  private void removeHtmlTags(Set<String> htmlFields, JSONObject processedJson, String key) {
    if(htmlFields.contains(key) && processedJson.get(key) != null) {
      try {
        processedJson.put(key, Html2Text.stripHtml((String)processedJson.get(key)));
      } catch (ExportStagingException e) {
        logger.warn("unable to remove html tags for key " + key , e);
      }
    }
  }

  /**
   * Removes null from json
   * 
   * @param processedJson the json to remove null element from
   * @param key key to process
   */
  private void removeNull(JSONObject processedJson, String key) {
    if (processedJson.get(key) == null) {
      processedJson.remove(key);
    }
  }
}
