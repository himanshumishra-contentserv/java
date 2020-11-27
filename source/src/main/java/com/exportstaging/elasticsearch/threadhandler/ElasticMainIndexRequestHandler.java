package com.exportstaging.elasticsearch.threadhandler;

import org.apache.log4j.Logger;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.IndexNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.exportstaging.elasticsearch.queryexecutor.ElasticSearchQueryExecutor;

@Component("elasticSearchRequestHandler")
public class ElasticMainIndexRequestHandler
{


  ///// INSTANCE VARS //////////////////////////////////////////////////////////////////////////////////////////////////


  @Value("${elasticsearch.connection.delay}")
  private int    retryDelay;
  @Value("${activemq.subscriber.elastic}")
  private String elasticSubscriber;

  @Autowired
  private ElasticSearchQueryExecutor queryExecutor;

  protected final Logger logger = Logger.getLogger("exportstaging");


  ///// PUBLIC METHODS /////////////////////////////////////////////////////////////////////////////////////////////////
  
  public Boolean executeBulkRequest(BulkRequest bulkRequest)
  {
    boolean insertRequestStatus = true;
    try {
      BulkResponse bulkResponse = queryExecutor.executeDocumentInsertRequest(bulkRequest);
      if (bulkResponse.hasFailures()) {
        insertRequestStatus = false;
      }
    } catch (IndexNotFoundException exception) {
      throw exception;
    } catch (NoNodeAvailableException | UnavailableShardsException noHostOrShard) {
      logger.error(
              "[" + elasticSubscriber + "] Unable to connect to ElasticSearch or Shard not available ."
                      + " Retrying to " + "perform operation" + noHostOrShard.getMessage()
      );
      waitForRetry();
      return executeBulkRequest(bulkRequest);
    } catch (Exception e) {
      logger.error("[ElasticMainIndexRequestHandler] Exception:" + e.getMessage());
      insertRequestStatus = false;
    }

    return insertRequestStatus;
  }


  ///// PRIVATE METHODS ////////////////////////////////////////////////////////////////////////////////////////////////


  private void waitForRetry()
  {
    try {
      Thread.sleep(retryDelay);
    } catch (InterruptedException interrupt) {
      Thread.currentThread().interrupt();
    }
  }
}
