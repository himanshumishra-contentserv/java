package com.exportstaging.elasticsearch.querybuilder;

import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.elasticsearch.ElasticOperationHandler;
import com.exportstaging.elasticsearch.queryexecutor.ElasticSearchQueryExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component("elasticIndexQueryBuilder")
public class ElasticIndexQueryBuilder extends ElasticReindexQueryBuilder
{

  ///// INSTANCE VARS //////////////////////////////////////////////////////////////////////////////////////////////////

  @Autowired
  private ElasticSearchQueryExecutor elasticQueryExecutor;
  @Autowired
  private ElasticOperationHandler handler;


  ///// PUBLIC METHODS /////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Method will prepare indices name and removes it from elasticsearch
   *
   * @param itemTypes       item type for which have to remove indices.
   * @param languageIds     Language ids for which remove indices.
   * @param subscriberName  elastic subscriber name
   *
   * @return boolean
   */
  public boolean prepareAndRemoveIndices(List<String> itemTypes, List<String> languageIds, String subscriberName)
  {
    List<String> indices = new ArrayList<>();

    for (String itemType : itemTypes) {
      if (ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType)) {
        for (String languageId : languageIds) {
          if (elasticSubscriber.equalsIgnoreCase(subscriberName)) {
            indices.add(handler.getElasticIndexName(itemType, languageId));
          }
          else {
            indices.add(handler.getSearchableIndexName(itemType, languageId));
          }
        }
      }
    }

    return elasticQueryExecutor.removeIndices(indices, handler.getSubscriberName());
  }
}
