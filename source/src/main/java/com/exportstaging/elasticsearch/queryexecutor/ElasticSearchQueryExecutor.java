package com.exportstaging.elasticsearch.queryexecutor;

import java.util.List;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;

import com.exportstaging.api.exception.ExportStagingException;

public interface ElasticSearchQueryExecutor
{
  /**
   * Re-Index request to reindex complete data from main index to searchable index
   *
   * @param builder object that contains request details
   *
   * @return response of executed request
   */
  BulkByScrollResponse executeReindexRequest(ReindexRequestBuilder builder);

  /**
   * Removes indices in bulk
   *
   * @param indicesToDelete List of index which have ot remove.
   *
   * @return true if successfully removed otherwise false
   */
  boolean removeIndices(List<String> indicesToDelete, String subscriberName);
  
  /**
   * Document insertion in main index will be done here
   *
   * @param bulkRequestBuilder object that consist of details of all request
   *
   * @return bulkResponse of executed bulkRequestBuilder
   *
   * @throws ExportStagingException 
   */
  BulkResponse executeDocumentInsertRequest(BulkRequest bulkRequest) throws Exception;

  /**
   * Checks for Index not found error message
   *
   * @param errorMessage error message
   * @return true if error message is Index not found otherwise false
   */
  boolean validateForIndexNotFound(String errorMessage);
}
