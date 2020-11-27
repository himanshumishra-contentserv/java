package com.exportstaging.elasticsearch.querybuilder;

import static com.exportstaging.common.ExportMiscellaneousUtils.TAB_DELIMITER;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.api.exception.ExceptionLogger;
import com.exportstaging.elasticsearch.ElasticOperationHandler;
import com.exportstaging.elasticsearch.clientbuilder.ClientBuilder;
import com.exportstaging.elasticsearch.queryexecutor.ElasticQueryExecutor;
import com.exportstaging.elasticsearch.updatedatamodel.ElasticIndexUpdater;
import com.exportstaging.elasticsearch.utils.ElasticsearchUtils;

@Component("elasticReindexQueryBuilder")
public class ElasticReindexQueryBuilder implements ReindexQueryBuilder
{

  ///// INSTANCE VARS //////////////////////////////////////////////////////////////////////////////////////////////////

  @Autowired
  private ClientBuilder           clientBuilder;
  @Autowired
  private ElasticQueryExecutor    queryExecutor;
  @Autowired
  private ElasticIndexUpdater     elasticIndexUpdater;
  @Autowired
  private ElasticOperationHandler elasticOperationHandler;

  protected final Logger logger = LogManager.getLogger("exportstaging");

  @Value("${activemq.subscriber.elastic}")
  protected String elasticSubscriber;
  @Value("${elasticsearch.connection.delay}")
  protected int    retryDelay;


  ///// PUBLIC METHODS /////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Re-Indexing for complete searchable index will be start from provided source index
   *
   * @param sourceIndexName      String name of source index
   * @param destinationIndexName String name of searchable index
   */
  public void reindexSearchableIndex(String sourceIndexName, String destinationIndexName, String[] searchableFields)
  {
    ReindexRequest request;
    try {
      request = getReindexRequestBuilder(sourceIndexName, destinationIndexName);
      if (!elasticIndexUpdater.getIsCS18OldVersion()) {
        RemoteInfo remoteInfo = getRemoteInfo();
        request.setRemoteInfo(remoteInfo);
      }
      request.getSearchRequest().source().fetchSource(searchableFields, null);
      BulkByScrollResponse response = clientBuilder.getClient(elasticSubscriber)
          .reindex(request, RequestOptions.DEFAULT);

    }catch (ElasticsearchStatusException exception) {
      handelIndexNotFoundException(sourceIndexName, exception);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      ExceptionLogger.logError(
        "Exception while reindexing data from source index [" + sourceIndexName + "] to destination index [" +
        destinationIndexName + "]",
        e, elasticSubscriber);
    }
  }


  /**
   * Prepare the ReindexRequestBuilder
   *
   * @param sourceIndexName      String name of source index
   * @param destinationIndexName String name of searchable index
   *
   * @return will return the prepared ReindexRequestBuilder
   */
  public ReindexRequest getReindexRequestBuilder(String sourceIndexName, String destinationIndexName)
    throws CannotCreateConnectionException
  {
    ReindexRequest request = new ReindexRequest(); 
    request.setSourceIndices(sourceIndexName);
    request.setDestIndex(destinationIndexName); 
    request.setAbortOnVersionConflict(false);
    request.setRefresh(true); 
   
    return request;
  }


  /**
   * From CS18.0 old indices(export_cslive_pdmarticle), new indices(export_cslive_pdmarticle_1,..) will be fill here
   *
   * @param sourceIndexName      String source index name
   * @param destinationIndexName String destination index name
   * @param languageId           String language id to fetch data from source index
   */
  public void reindexMainIndex(String sourceIndexName, String destinationIndexName, String languageId)
  {
    ReindexRequest request = null;
    try {
      request = getReindexRequestBuilder(sourceIndexName, destinationIndexName);
      request.setScript(
        new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "ctx._id = ctx._source[\"ID\"]", new HashMap<>()));
      if (!elasticIndexUpdater.getIsCS18OldVersion()) {
        RemoteInfo remoteInfo = getRemoteInfo();
        request.setRemoteInfo(remoteInfo);
      }
      else {
        request.setSourceQuery(getFilter("LanguageID", languageId));
      }
      BulkByScrollResponse response = clientBuilder.getClient(elasticSubscriber)
          .reindex(request, RequestOptions.DEFAULT);
    } catch (ElasticsearchStatusException exception) {
      handelIndexNotFoundException(sourceIndexName, exception);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      ExceptionLogger.logError(
              "Exception while reindexing data from source index [" + sourceIndexName + "] to destination index [" + destinationIndexName + "]",
              e, elasticSubscriber);
    }
  }


  private void handelIndexNotFoundException(String sourceIndexName, ElasticsearchStatusException exception) {
    if (exception.status().name().contains("NOT_FOUND")) {
      if (exception.getMessage().contains("index_not_found_exception")) {
        System.out.println("Source index '" + sourceIndexName + "' does not exists.");
      }
    }
  }


  /**
   * Delete the old index from where migration is done
   *
   * @param indexName Name of the index
   */
  public void deleteOldIndex(String indexName) throws CannotCreateConnectionException
  {
    if (!elasticIndexUpdater.getIsCS18OldVersion()) {
      Map<String, String> cs17ElasticCredentials = elasticIndexUpdater.getCs17ElasticCredentials();
      String host = cs17ElasticCredentials.get(ElasticsearchUtils.EXPORT_CS17_ELASTIC_HOST);
      String port = cs17ElasticCredentials.get(ElasticsearchUtils.EXPORT_CS17_ELASTIC_PORT);
      String curl = "curl -XDELETE http://" + host + ":" + port + "/" + indexName;
      try {
        Process p = Runtime.getRuntime().exec(curl);
        p.waitFor();
      } catch (Exception e) {
        ExceptionLogger.logError("Error while deleting index after migration[" + indexName + "]", e, elasticSubscriber);
      }
    }
    else {
      RestHighLevelClient client = clientBuilder.getClient(elasticSubscriber);
      try {
	    client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
	  } catch (IOException e) {
	    ExceptionLogger.logError("Error while deleting index after migration[" + indexName + "]", e, elasticSubscriber);
	  }
    }
  }


  /**
   * Re-Indexing for provided document IDs
   *
   * @param sourceIndexName      String name of source index
   * @param destinationIndexName String name of searchable index
   * @param documentIds          String Id of the document
   */
  public boolean reindexSearchableIndex(
    String sourceIndexName,
    String destinationIndexName,
    String languageId,
    List<String> documentIds,
    Set<String> searchableFields
  ) throws CannotCreateConnectionException
  {
    ReindexRequest request = null;
    try {
      request = getReindexRequestBuilder(sourceIndexName, destinationIndexName);
      request.setSourceQuery(getFilter(ElasticsearchUtils.ES_FIELDS_ID, documentIds));
      //Converted SearchableFields from Set to array
      String[] searchableFieldsArray = searchableFields.stream().toArray(String[]::new);
      request.getSearchRequest().source().fetchSource(searchableFieldsArray, null);

      logger.info("The reindex operation started for index [" + destinationIndexName + "] ");
      BulkByScrollResponse response = clientBuilder.getClient(elasticSubscriber)
          .reindex(request, RequestOptions.DEFAULT);

      if (response.getBulkFailures().size() == 0 && response.getSearchFailures().size() == 0) {
        logger.info("The reindex operation successfully completed for index [" + destinationIndexName + "] ");
        return true;
      }
      logFailures(response);

    } catch (IndexNotFoundException exception) {
      throw exception;
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      ExceptionLogger.logError("Connection to Elasticsearch failed. Retrying to perform operation [Reindex]", noHostOrShard, elasticSubscriber);
      try {
        Thread.sleep(retryDelay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return reindexSearchableIndex(sourceIndexName, destinationIndexName, languageId, documentIds, searchableFields);
    } catch (ElasticsearchStatusException e) {
      logger.warn("Connection to Elasticsearch failed. Retrying to perform operation [Reindex]", e.getCause());
      if(queryExecutor.increaseMappingFieldsLimit(destinationIndexName, e.getCause().getMessage())) {
        return reindexSearchableIndex(sourceIndexName, destinationIndexName, languageId, 
          documentIds, searchableFields);
      }
    } catch (IllegalStateException e) {
      if (e.getCause() instanceof InterruptedException) {
        if (e.getMessage().equalsIgnoreCase("Future got interrupted [Reindex]")) {
          Thread.currentThread().interrupt();
        }
      }
    } catch (CannotCreateConnectionException e) {
      //Note: throw e because otherwise it will be caught in below Exception block
      throw e;
    } catch (Exception e) {
      ExceptionLogger.logError("Exception while Reindexing.", e, elasticSubscriber);
    }

    return false;
  }


  ///// PRIVATE METHODS ////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Prepares term filter condition for reindex query
   *
   * @param fieldName  String  Field name
   * @param fieldValue String  Field value
   *
   * @return QueryBuilder having term query condition
   */
  private QueryBuilder getFilter(String fieldName, String fieldValue)
  {
    QueryBuilder queryBuilder = QueryBuilders.termQuery(fieldName, fieldValue);

    return queryBuilder;
  }


  /**
   * Prepare terms filter condition for reindex query, appended all the affected ItemIDs
   *
   * @param fieldName  String name of the field
   * @param fieldValue String Value of the filed
   *
   * @return QueryBuilder having provided condition
   */
  private QueryBuilder getFilter(String fieldName, List<String> fieldValue)
  {
    QueryBuilder queryBuilder = QueryBuilders.termsQuery(fieldName, fieldValue);

    return queryBuilder;
  }
  
  /**
   * log failures of reindex operation
   * @param response
   */
  private void logFailures(BulkByScrollResponse response) {
    response.getBulkFailures().forEach(x -> {
      ExceptionLogger.logError("Reindex bulk operation failed for index " + x.getIndex()
          + " with message" + x.getMessage(), x.getCause(), elasticSubscriber);
      });
      response.getSearchFailures().forEach(x-> {
        ExceptionLogger.logError("Reindex search operation failed for index " + x.getIndex()
              + " with message" + x.getReason(), x.getReason(), elasticSubscriber);  
      });
  }


  /**
   * Prepare the RemoteInfo object to connect Remote Elastic Search
   *
   * @return RemoteInfo Object of RemoteInfo with remote elastic search creadentials
   */
  private RemoteInfo getRemoteInfo()
  {
    Map<String, String> headers = new HashMap<>();

    // TODO: find proper value for header
    headers.put("test", "test");
    Map<String, String> cs17ElasticCredentials = elasticIndexUpdater.getCs17ElasticCredentials();
    RemoteInfo remoteInfo = new RemoteInfo(
      ElasticsearchUtils.EXPORT_CS17_ELASTIC_SCHEME,
      cs17ElasticCredentials.get(ElasticsearchUtils.EXPORT_CS17_ELASTIC_HOST),
      Integer.parseInt(cs17ElasticCredentials.get(ElasticsearchUtils.EXPORT_CS17_ELASTIC_PORT)),
      null,
      new BytesArray(new MatchAllQueryBuilder().toString()),
      cs17ElasticCredentials.get(ElasticsearchUtils.EXPORT_CS17_ELASTIC_USER_NAME),
      cs17ElasticCredentials.get(ElasticsearchUtils.EXPORT_CS17_ELASTIC_PASSWORD),
      headers,
      new TimeValue(100, TimeUnit.SECONDS),
      new TimeValue(1000, TimeUnit.SECONDS)
    );

    return remoteInfo;
  }
}
