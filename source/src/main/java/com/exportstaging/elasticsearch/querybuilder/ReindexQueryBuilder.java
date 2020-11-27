package com.exportstaging.elasticsearch.querybuilder;

import com.exportstaging.api.exception.CannotCreateConnectionException;

import java.util.List;
import java.util.Set;

public interface ReindexQueryBuilder
{
  /**
   * Re-Indexing for complete searchable index will be start from provided source index
   *
   * @param sourceIndexName      String name of source index
   * @param destinationIndexName String name of searchable index
   */
  void reindexSearchableIndex(String sourceIndexName, String destinationIndexName, String[] searchableFields);


  /**
   * From CS18.0 old indices(export_cslive_pdmarticle), new indices(export_cslive_pdmarticle_1,..) will be fill here
   *
   * @param sourceIndexName String source index name
   * @param targetIndexName String destination index name
   * @param languageId      String language id to fetch data from source index
   */
  void reindexMainIndex(String sourceIndexName, String targetIndexName, String languageId);


  /**
   * Re-Indexing for the Searchable Index according to the document Ids Provided
   *
   * @param sourceIndexName      String source index name
   * @param destinationIndexName String destination index name
   * @param documentId           String document ID in the main index to be sent to the Searchable index
   */
  boolean reindexSearchableIndex(
    String sourceIndexName,
    String destinationIndexName,
    String languageId,
    List<String> documentId,
    Set<String> searchableFields
  ) throws CannotCreateConnectionException;


  /**
   * Delete the old index from where migration is done
   *
   * @param indexName Name of the index
   */
  void deleteOldIndex(String indexName) throws CannotCreateConnectionException;
}
