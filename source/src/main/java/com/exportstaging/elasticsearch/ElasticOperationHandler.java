package com.exportstaging.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.connectors.idbconnector.IntermediateDAO;
import com.exportstaging.elasticsearch.clientbuilder.ClientBuilder;
import com.exportstaging.elasticsearch.utils.ElasticsearchUtils;

/**
 * All the elastic related method that could be used from many places are placed inside this class
 */
@Component
public class ElasticOperationHandler
{

  ///// INSTANCE VARS //////////////////////////////////////////////////////////////////////////////////////////////////

  @Value("${elasticsearch.index.prefix}")
  private String elasticIndexPrefix;
  @Value("${elasticsearch.table.filecontent}")
  private String fileContentTypeName;
  @Value("${elasticsearch.table}")
  private String itemTypeName;
  @Value("${core.project.name}")
  private String projectName;
  @Value("${elasticsearch.connection.delay}")
  private int    retryDelay;
  @Value("${activemq.subscriber.elastic}")
  private String elasticSubscriber;

  @Autowired
  ClientBuilder clientBuilder;
  @Autowired
  protected IntermediateDAO intermediateDAO;

  public static   boolean isLanguagePerIndex = false;
  protected final Logger  logger             = LogManager.getLogger("exportstaging");
  private String subscriberName;


  ///// PUBLIC METHODS /////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Method will provide name of an elastic search index name according to provided item type
   *
   * @param itemType String  item type could be anyone from configured item types like. Pdmarticle/Mamfile/User
   *
   * @return Name of elastic search index
   */
  public String getElasticIndexName(String itemType)
  {
    return elasticIndexPrefix + "_" + itemType.trim().toLowerCase();
  }


  /**
   * Method will provide name of an elastic search index name according to provided item type and
   * languageID
   *
   * @param itemType   String item type could be anyone from configured item types like. Pdmarticle/Mamfile/User
   * @param languageId String ContentServ language id.
   *
   * @return Name of elastic search index
   */
  public String getElasticIndexName(String itemType, String languageId)
  {
    String indexName = getElasticIndexName(itemType);
    if (!itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT)) {
      indexName = indexName + "_" + languageId;
    }
    return indexName;
  }


  /**
   * As per the provided object type and language id, searchable index name will be return
   *
   * @param itemType   String object type
   * @param languageId String language id
   *
   * @return name of searchable index
   */
  public String getSearchableIndexName(String itemType, String languageId)
  {
    String indexName       = getElasticIndexName(itemType);
    String searchIndexName = "";
    if (!itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT)) {
      searchIndexName = indexName + "_" + "search" + "_" + languageId;
    }

    return searchIndexName;
  }


  /**
   * Method will provide name of type(Item or FileContent) according to provided item type
   *
   * @param type String name of type
   *
   * @return name of type(Item or FileContent) according to provided item type
   */
  public String getElasticTypeName(String type)
  {
    switch (type) {
      case ElasticsearchUtils.ES_CONSTANT_ITEM:
        return itemTypeName;
      case ElasticsearchUtils.ES_CONSTANT_FILECONTENT:
        return fileContentTypeName;
    }
    return null;
  }


  /**
   * flag will be set to determine the indices
   * if the previous are exists then it should be false
   * if previous indices are not exists then it should be true
   */
  public void updateLanguagePerIndexFlag(String subscriberName) {
    setSubscriberName(subscriberName);
    List<String> coreItemTypes = getCoreItemType();
    boolean isLanguagePerIndex = true;
    for (String objectType : coreItemTypes) {
      String indexName = elasticIndexPrefix + "_" + objectType.toLowerCase();
      if (isIndexExisting(objectType, indexName)) {
        isLanguagePerIndex = false;
        break;
      }
    }

    setLanguagePerIndexFlag(isLanguagePerIndex);
  }


  public String getSubscriberName()
  {
    return subscriberName;
  }


  public void setSubscriberName(String subscriber)
  {
    subscriberName = subscriber;
  }


  /**
   * Method will return all the configured core object types
   *
   * @return list of all the core configured object types
   */
  public List<String> getCoreItemType()
  {
    List<String> coreItemTypes = new ArrayList<>();
    coreItemTypes.addAll(ExportMiscellaneousUtils.convertStringToList(
      ExportMiscellaneousUtils.getPropertyValue("export.core.itemtypes", ExportMiscellaneousUtils.CORE_PROPERTIES,
                                                projectName)));

    return coreItemTypes;
  }


  public Set<String> getSearchableFields(String objectType)
  {
    Set<String> searchableFields = intermediateDAO.getSearchableAndStandardFields(objectType);

    return searchableFields;
  }


  public Set<String> getSearchableFields(String objectType, boolean includeStandardFields)
  {
    return includeStandardFields
           ? intermediateDAO.getSearchableAndStandardFields(objectType)
           : intermediateDAO.getSearchableFields(objectType);
  }


  /**
   * Get searchable references fields
   *
   * @param objectType String Type of the Object
   * @return Set of searchable references fields
   */
  public Set<String> getSearchableReferenceFields(String objectType) {
    return intermediateDAO.getSearchableReferences(objectType);
  }


  /**
   * Get searchable subtable fields
   *
   * @param objectType String Type of the Object
   * @return Set of searchable subtable fields
   */
  public Set<String> getSearchableSubtableFields(String objectType) {
    return intermediateDAO.getSearchableSubtables(objectType);
  }


  /**
   * flag will be set only when subscriber will start
   * true means new indices(export_pdmarticle_1) needs to consider
   * otherwise old indices
   *
   * @param flag boolean value to determine indices
   */
  public void setLanguagePerIndexFlag(boolean flag)
  {
    isLanguagePerIndex = flag;
  }


  /**
   * provide the value to determine which indices should get used
   * if its true then new indices(export_pdmarticle_1) will be consider
   * otherwise old indices(export_pdmarticle) will be used
   *
   * @return provide flag to determine indices
   */
  public boolean getLanguagePerIndexFlag()
  {
    return isLanguagePerIndex;
  }

  /**
   * Use to exclude the record type from the provided list of object types if it has any
   *
   * @param configuredObjectTypes list from which record type needs to exclude
   *
   * @return list containing all the provided object type excluding record types
   */
  public List<String> avoidRecordProcessing(List<String> configuredObjectTypes)
  {
    configuredObjectTypes.removeAll(getRecordObjectTypes());

    return configuredObjectTypes;
  }


  /**
   * Use to exclude the mamfilecontent from the provided list of object types if it has any
   *
   * @param objectType list of provided object
   *
   * @return list containing all the provided object type excluding mamfilecontent
   */
  public List<String> avoidMamfileContentType(List<String> objectType)
  {
    objectType.remove(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT);

    return objectType;
  }


  /**
   * Provide list of record types
   *
   * @return list of record
   */
  public List<String> getRecordObjectTypes()
  {
    List<String> recordObjectTypes = new ArrayList<>(ExportMiscellaneousUtils.getCoreRecordTypes());

    return recordObjectTypes;
  }


  public boolean deleteIndices(String itemType, String subscriberName)
  {
    boolean deleteFlag = true;
    if (getLanguagePerIndexFlag()) {
      List<String> languageIDs = intermediateDAO.getLanguageIds();
      for (String languageID : languageIDs) {
        String indexName;
        if (subscriberName.equalsIgnoreCase("ElasticSubscriber")) {
          indexName = getElasticIndexName(itemType, languageID);
        }
        else {
          indexName = getSearchableIndexName(itemType, languageID);
        }
        if (isIndexExisting(itemType, indexName)) {
          deleteFlag = deleteFlag & deleteIndex(indexName);
        }
      }
    }
    else {
      String indexName = getElasticIndexName(itemType);
      if (isIndexExisting(itemType, indexName)) {
        deleteFlag = deleteIndex(getElasticIndexName(itemType));
      }
    }

    return deleteFlag;
  }


  /**
   * Check the provided index is exists in Elastic Search or not
   *
   * @param itemType  String type of object
   * @param indexName String Name of the index
   *
   * @return true if provided index is exists in Elastic Search otherwise false
   */
  public boolean isIndexExisting(String itemType, String indexName)
  {
    return checkIndexExistOrNot(itemType, indexName);
  }


  public List<String> getElasticSupportedItemTypes(List<String> itemTypes)
  {
    if (itemTypes.contains(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILE)) {
      itemTypes.add(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT);
    }

    return itemTypes;
  }


  public Set<String> getSearchableItemHeaders(String itemType)
  {
    return getSearchableFields(itemType);
  }


  public void logError(String message, Exception e)
  {
    logger.error(
      "[" + elasticSubscriber + "]:" + message + " Error Message:" + e.getMessage() + ExportMiscellaneousUtils
        .TAB_DELIMITER + Arrays
        .toString(e.getStackTrace()));
  }
  
  /**
   * Get Fields of give cs type
   * @param objectType cs item type
   * @param type cs type of a field
   * @param searchablility only searchable or all custom fields
   * @return set of fields of given cs type
   */
  public Set<String> getFields(String itemType, String type, String searchablility) {

    return intermediateDAO.getFields(itemType, type, searchablility);

  }

  ///// PRIVATE METHODS ////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Check the provided index is exists in Elastic Search or not
   *
   * @param itemType  String type of object
   * @param indexName String Name of the index
   *
   * @return true if provided index is exists in Elastic Search otherwise false
   */
  private boolean checkIndexExistOrNot(String itemType, String indexName)
  {
    boolean exists = false;
    if (!"".equals(indexName)) {
      try {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        exists = clientBuilder.getIndicesAdminClient(getSubscriberName()).exists(getIndexRequest, RequestOptions.DEFAULT);
      } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
        logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
        try {
          Thread.sleep(retryDelay);
        } catch (InterruptedException interrupt) {
          Thread.currentThread().interrupt();
        }
        return checkIndexExistOrNot(itemType, indexName);
      } catch (java.lang.IllegalStateException e) {
        if (e.getCause() instanceof InterruptedException) {
          if (e.getMessage().equalsIgnoreCase("Future got interrupted")) {
            Thread.currentThread().interrupt();
          }
        }
      } catch (Exception e) {
        logError("Exception while checking index[" + indexName + "]", e);
      }
    }

    return exists;
  }


  private boolean deleteIndex(String indexName)
  {
    boolean acknowledged = false;
    try {
      DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);	
      acknowledged = clientBuilder.getIndicesAdminClient(getSubscriberName())
                                  .delete(deleteIndexRequest, RequestOptions.DEFAULT)
                                  .isAcknowledged();
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      logError("Connection to Elasticsearch failed." + " Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return deleteIndex(indexName);
    } catch (java.lang.IllegalStateException e) {
      if (e.getCause() instanceof InterruptedException) {
        if (e.getMessage().equalsIgnoreCase("Future got interrupted")) {
          Thread.currentThread().interrupt();
        }
      }
    } catch (Exception e) {
      logError("Exception while deleting index[" + indexName + "]", e);
    }

    return acknowledged;
  }
}
