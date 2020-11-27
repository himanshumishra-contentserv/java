package com.exportstaging.elasticsearch.queryexecutor;

import static com.exportstaging.common.ExportMiscellaneousUtils.TAB_DELIMITER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.elasticsearch.clientbuilder.ClientBuilder;
import com.exportstaging.elasticsearch.utils.ElasticsearchUtils;

@Component
public class ElasticQueryExecutor implements ElasticSearchQueryExecutor
{


  ///// INSTANCE VARS //////////////////////////////////////////////////////////////////////////////////////////////////


  @Value("${activemq.subscriber.elastic}")
  private String elasticSubscriber;
  @Value("${elasticsearch.connection.delay}")
  private int    retryDelay;

  @Autowired
  private ClientBuilder clientBuilder;

  protected final Logger logger = LogManager.getLogger("exportstaging");


  ///// PUBLIC METHODS /////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Re-Index request to reindex complete data from main index to searchable index
   *
   * @param builder object that contains request details
   *
   * @return response of executed request
   */
  public BulkByScrollResponse executeReindexRequest(ReindexRequestBuilder builder)
  {
    BulkByScrollResponse response = null;
    try {
      response = builder.execute().actionGet();
      processReindexResponse(builder, response);
    } catch (IndexNotFoundException exception) {
      throw exception;
    } catch (Exception exception) {
      throw exception;
    }

    return response;
  }


  /**
   * As per the received response, if we need to increase the field limit it will increase it
   *
   * @param builder  object that need to process again once the field limit increase
   * @param response object that needs to validate
   */
  public void processReindexResponse(ReindexRequestBuilder builder, BulkByScrollResponse response)
  {
    List bulkFailure = response.getBulkFailures();
    if (!bulkFailure.isEmpty()) {
      BulkItemResponse.Failure bulkFailureMessage = (BulkItemResponse.Failure) bulkFailure.get(0);
      String                   cause              = bulkFailureMessage.getMessage();
      String                   indexName          = bulkFailureMessage.getIndex();
      if (increaseMappingFieldsLimit(indexName, cause)) {
        executeReindexRequest(builder);
      }
    }/* else if (response.getBatches() > 0 && response.getCreated() == 0 && response.getDeleted() == 0 && response
    .getUpdated() == 0) {
            waitForTime();
            executeReindexRequest(builder);
        }*/
  }


  /**
   * Document insertion in main index will be done here
   *
   * @param bulkRequestBuilder object that consist of details of all request
   *
   * @return bulkResponse of executed bulkRequestBuilder
 * @throws Exception 
   */
  public BulkResponse executeDocumentInsertRequest(BulkRequest bulkRequest) throws Exception
  {
    BulkResponse bulkResponse   = null;
    List <String> indexNames    = new ArrayList<>();
    boolean indexNotFoundStatus = false;
    try {
      bulkResponse = clientBuilder.getClient(elasticSubscriber).bulk(bulkRequest, RequestOptions.DEFAULT);
      if (bulkResponse.hasFailures()) {
        logger.warn(bulkResponse.buildFailureMessage());
        BulkItemResponse bulkItemResponse[] = bulkResponse.getItems();
        for (BulkItemResponse bulkItemResponseTemp : bulkItemResponse) {
          String failedMessage = bulkItemResponseTemp.getFailureMessage();
          if (failedMessage != null && StringUtils.isNotEmpty(failedMessage)
                  && validateForMappingFieldsLimit(failedMessage)) {
            String indexName = bulkItemResponseTemp.getIndex();
            increaseMappingFieldsLimit(indexName, failedMessage);
          }
          if (failedMessage != null && StringUtils.isNotEmpty(failedMessage)
                  && validateForIndexNotFound(failedMessage)) {
            String indexName = bulkItemResponseTemp.getIndex();
            if (!indexNames.contains(indexName)) indexNames.add(indexName);
            indexNotFoundStatus = true;
          }
        }
        if (indexNotFoundStatus) {
          IndexNotFoundException indexNotFoundException = new IndexNotFoundException(ExportMiscellaneousUtils.EXPORT_ELASTIC_INDEX_NOT_FOUND_MESSAGE);
          indexNotFoundException.addHeader("indexNames", indexNames);
          throw indexNotFoundException;
        }
        executeDocumentInsertRequest(bulkRequest);
      }
    } catch (IndexNotFoundException exception) {
      logger.info("Indexes/Index " + indexNames.toString() + " does not exist, so we are creating those indexes and trying again");
      throw exception;
    } catch (Exception exception) {
      logger.error(exception.getMessage());
      throw exception;
    }

    return bulkResponse;
  }
  

  /**
   * Removes indices in bulk
   *
   * @param indicesToDelete List of index which have ot remove.
   *
   * @return true if successfully removed otherwise false
   */
  public boolean removeIndices(List<String> indicesToDelete, String subscriberName)
  {
    boolean acknowledged = false;
    try {
      
      DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indicesToDelete.toArray(new String[0]));
      acknowledged = clientBuilder.getIndicesAdminClient(subscriberName)
          .delete(deleteIndexRequest, RequestOptions.DEFAULT).isAcknowledged();
                                                             
    } catch (java.lang.IllegalStateException e) {
      if (e.getCause() instanceof InterruptedException) {
        if (e.getMessage().equalsIgnoreCase("Future got interrupted")) {
          Thread.currentThread().interrupt();
        }
      }
    } catch (Exception e) {
      logError("Exception while deleting indices.", e);
    }
    return acknowledged;
  }

  /**
   * Default fields limit is 1000 for per index but if its exceeded then it would be set to new limit
   * New limit would be current limit + 50% of current limit
   *
   * @param indexName      String   type of an item
   * @param failureMessage String message that we need to check before setting new limit
   */
  public boolean increaseMappingFieldsLimit(String indexName, String failureMessage)
  {
    if (validateForMappingFieldsLimit(failureMessage)) {
      int currentMappingFieldsLimit = getIndexSettingValue(indexName, ElasticsearchUtils.ES_SETTING_FIELD_LIMIT);
      int newMappingFieldsLimit     = (currentMappingFieldsLimit + (currentMappingFieldsLimit / 2));
      try {
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
        request.settings(Settings.builder().put(ExportMiscellaneousUtils.EXPORT_ELASTIC_SETTING_FIELD_LIMIT, 
            newMappingFieldsLimit));
        clientBuilder.getIndicesAdminClient(elasticSubscriber).putSettings(request, RequestOptions.DEFAULT);
      } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
        logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
        try {
          Thread.sleep(retryDelay);
        } catch (InterruptedException interrupt) {
          Thread.currentThread().interrupt();
        }
        return increaseMappingFieldsLimit(indexName, failureMessage);
      } catch (Exception e) {
        logError("Exception while increasing field limit.", e);
      }
      logger.warn(
        "[" + elasticSubscriber + "]: " + ExportMiscellaneousUtils.EXPORT_ELASTIC_FIELD_LIMIT + newMappingFieldsLimit + " for the index: " + indexName);
      return true;
    }
    return false;
  }
  
  
  ///// PRIVATE METHODS ////////////////////////////////////////////////////////////////////////////////////////////////


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
      GetSettingsResponse response = clientBuilder.getIndicesAdminClient(elasticSubscriber)
          .getSettings(request, RequestOptions.DEFAULT);
      for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
        Settings settings = cursor.value;
        currentMappingFieldLimit = settings.getAsInt(settingName, ElasticsearchUtils.ES_DEFAULT_MAPPING_FIELD);
      }
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      logError("Connection to Elasticsearch failed. Retrying to perform operation.", noHostOrShard);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      return getIndexSettingValue(indexName, settingName);
    } catch (Exception e) {
      logError("Exception while retrieving settings value.", e);
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
   * Checks for Index not found error message
   * @param errorMessage error message
   *
   * @return true if error message is Index not found otherwise false
   */
  public boolean validateForIndexNotFound(String errorMessage) {
    return errorMessage.contains(ExportMiscellaneousUtils.EXPORT_ELASTIC_INDEX_NOT_FOUND_MESSAGE);
  }


  /**
   * Method will add provided message to the logger that will log into file
   *
   * @param message String message that need to write
   * @param e       object of exception
   */
  private void logError(String message, Exception e)
  {
    logger.error("[" + elasticSubscriber + "]:" + message + " Error Message:" + e.getMessage());
    logger.debug("[" + elasticSubscriber + "]:" + message + " Error Message:" + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
  }
}
