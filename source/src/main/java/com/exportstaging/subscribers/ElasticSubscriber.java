package com.exportstaging.subscribers;

import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.index.IndexNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.api.exception.ExceptionLogger;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.domain.ItemMessage;
import com.exportstaging.domain.Message;
import com.exportstaging.domain.OperationMessage;
import com.exportstaging.elasticsearch.ElasticOperationHandler;
import com.exportstaging.elasticsearch.ElasticSearchOperations;
import com.exportstaging.elasticsearch.clientbuilder.ClientBuilder;
import com.exportstaging.elasticsearch.querybuilder.ReindexQueryBuilder;
import com.exportstaging.elasticsearch.utils.ElasticsearchUtils;

@Component
public class ElasticSubscriber extends AbstractSubscriber {

    @Autowired
    private ElasticSearchOperations elasticSearchOperations;
    @Autowired
    private ClientBuilder clientBuilder;
    @Autowired
    private ElasticOperationHandler handler;
    @Autowired
    @Qualifier("elasticReindexQueryBuilder")
    private ReindexQueryBuilder reindexQueryBuilder;
    @Value("${activemq.subscriber.elastic}")
    String elasticSubscriber;

    @Value("${elasticsearch.reindex.batchsize}")
    public int batchSize;

    public ElasticSubscriber() {
    }

    public void startSubscriber() throws CannotCreateConnectionException {
        if (subscriberName.equalsIgnoreCase("SearchableElasticSubscriber") && !handler.isLanguagePerIndex) {
            return;
        }
        logger = LogManager.getLogger(ExportMiscellaneousUtils.EXPORT_LOGGER_NAME);
        String messageInfo = "-------------------" + subscriberName + " Initialization---------------------";
        System.out.println(messageInfo);
        logger.info(messageInfo);
        if (getHandledItemTypes().isEmpty()) {
            logger.info("[" + subscriberName + "] No Items configured for initialize index");
        }
        handler.updateLanguagePerIndexFlag(subscriberName);
        elasticSearchOperations.prepareThreadPool();
        initializeSearchEngine(handler.getElasticSupportedItemTypes(getHandledItemTypes()), this.subscriberName);
        super.startSubscriber();
    }


    public void initializeSearchEngine(List<String> itemTypes, String subscriberName) throws CannotCreateConnectionException {
        for (String itemType : itemTypes) {
            boolean shouldCheckIndex = false;
            if (ExportMiscellaneousUtils.getConfiguredTypes().contains(itemType)
                    && ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType)) {
                shouldCheckIndex = true;
            } else if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT)) {
                shouldCheckIndex = true;
            }
            if (shouldCheckIndex) {
                createIndex(itemType, subscriberName);
            }
        }
    }


    @Override
    public List<String> getHandledItemTypes() {
        return new ArrayList<>(ExportMiscellaneousUtils.getConfiguredTypes());
    }


  @Override
  public Boolean processMessage(ItemMessage itemMessage, String itemType, String type, String projectName)
  {
    if (type.equals(typeItem)) {
      return elasticSearchOperations.addDocumentForAllLanguages(itemMessage, itemType, type, subscriberName);
    }
    else if (type.equals(typeFileContent)) {
      return elasticSearchOperations.addDocument(
        itemMessage.getRawMessage(),
        type,
        itemMessage.getId(),
        subscriberName
      );
    }

    return false;
  }

    @Override
    public Boolean processMessage(Message message, String itemType, String type, String projectName) {
        if (ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE.equalsIgnoreCase(itemType) && handler.getLanguagePerIndexFlag() && message.getAction() == actionCreate) {
            List<String> languageIdList = new ArrayList<>();
            String indexName;
            languageIdList.add(message.getId());
            for (String supportedItemType : getHandledItemTypes()) {
                if (ExportMiscellaneousUtils.getCoreItemTypes().contains(supportedItemType)) {
                    for (String languageID : languageIdList) {
                        if (elasticSubscriber.equalsIgnoreCase(subscriberName)) {
                            indexName = handler.getElasticIndexName(supportedItemType, languageID);
                        } else {
                            indexName = handler.getSearchableIndexName(supportedItemType, languageID);
                        }
                            try {
                                validateAndCreateIndex(supportedItemType, indexName);
                            } catch (CannotCreateConnectionException e) {
                                ExceptionLogger.logError(e.getMessage(), e, elasticSubscriber);
                                return false;
                            }
                        }
                }
            }
        }
        return true;
    }

    @Override
    public Boolean deleteMessage(List<String> ids, String itemType, String type, String projectName) {
        int deleteStatus = 0;
        if (type.equals(typeItem) && ExportMiscellaneousUtils.getCoreItemTypes().contains(itemType)) {
            deleteStatus = elasticSearchOperations.deleteDocumentForAllLanguages(ids, itemType, subscriberName);
        } else if (type.equals(typeFileContent)) {
            deleteStatus = elasticSearchOperations.deleteDocument(ids, type, subscriberName);
        } else if (handler.getLanguagePerIndexFlag() && ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_LANGUAGE.equalsIgnoreCase(itemType)) {
            deleteStatus = elasticSearchOperations.removeIndices(getHandledItemTypes(), Arrays.asList(ids.get(0)), subscriberName) ? 1 : 0;
        } else if (ExportMiscellaneousUtils.getCoreRecordTypes().contains(itemType)) {
            //TODO Perform reindex for searchable index from main index
            //Issue: DocumentSourceMissingException due to _source disable
            if (subscriberName.equalsIgnoreCase("SearchableElasticSubscriber")) {
                deleteStatus = 1;
            } else {
                deleteStatus = elasticSearchOperations.updateAffectedItem(ids, itemType, subscriberName);
            }
        }
        return deleteStatus != 0;
    }

    @Override
    public boolean operationCleanup() {
        if (removedItemTypes(ExportMiscellaneousUtils.splitItemTypes(exportSupportedItemTypes))) {
            logger.info("[" + subscriberName + "] Data cleanup complete.");
            return true;
        } else {
            logger.error("[" + subscriberName + "] Failed to cleanup data. Rectify this as this may lead to data loss.");
            return false;
        }
    }

    @Override
    public boolean removedItemTypes(List<String> itemTypes) {
        for (String itemType : handler.getElasticSupportedItemTypes(itemTypes)) {
            if (itemType.equalsIgnoreCase("Workflow") || itemType.equalsIgnoreCase("Language")) {
                continue;
            } else if (elasticSearchOperations.removeIndex(itemType, subscriberName)) {
                logger.info("[" + subscriberName + "] Index deleted for ItemType: " + itemType);
            } else {
                logger.info("[" + subscriberName + "] Failed to delete Index for ItemType: " + itemType);
            }
        }
        return true;
    }

    @Override
    public boolean operationInitialize(List<String> itemTypes) {
        try {
            initializeSearchEngine(handler.getElasticSupportedItemTypes(itemTypes), this.subscriberName);
        } catch (CannotCreateConnectionException e) {
            return false;
        }
        return true;
    }


    @Override
    public void onBeforeShutdown() {
        clientBuilder.closeClient();
    }


    /**
     * Method will be responsible to create elastic indexes if its not exists
     *
     * @param itemType item type could be anyone from configured item types
     */
    private void createIndex(String itemType, String subscriberName) throws CannotCreateConnectionException {
        if (handler.getLanguagePerIndexFlag()) {
            if (itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT)
                    && elasticSubscriber.equalsIgnoreCase(subscriberName)) {
                String indexName = handler.getElasticIndexName(itemType);
                validateAndCreateIndex(itemType, indexName);
            } else if (!itemType.equalsIgnoreCase(ExportMiscellaneousUtils.EXPORT_ITEM_TYPE_MAMFILECONTENT)) {
                List<String> languageIDs = intermediateDAO.getLanguageIds();
                for (String languageID : languageIDs) {
                    String indexName;
                    if (elasticSubscriber.equalsIgnoreCase(subscriberName)) {
                        indexName = handler.getElasticIndexName(itemType, languageID);
                    } else {
                        indexName = handler.getSearchableIndexName(itemType, languageID);
                    }
                    validateAndCreateIndex(itemType, indexName);
                }
            }
        } else {
            String indexName = handler.getElasticIndexName(itemType);
            boolean validateIndex = elasticSearchOperations.validateIndex(itemType, indexName);
            if (!validateIndex) {
                itemType = itemType.toLowerCase();
                elasticSearchOperations.initializeIndex(itemType, indexName, getItemHeader(itemType), subscriberName);
            }
        }
    }


    /**
     * Method will be responsible to validate the indexes and create elastic indexes
     *
     * @param itemType  item type could be anyone from configured item types
     * @param indexName Name of the index
     */
    public void validateAndCreateIndex(String itemType, String indexName) throws CannotCreateConnectionException {
        boolean validateIndex = elasticSearchOperations.validateIndex(itemType, indexName);
        if (!validateIndex) {
            itemType = itemType.toLowerCase();
            elasticSearchOperations.initializeIndex(itemType, indexName, getItemHeader(itemType),subscriberName);
        }
    }

    @Override
    public boolean operationTypeReindex(OperationMessage operationMessage, List<String> languageIds) {
        if (!handler.getLanguagePerIndexFlag())
           return true;

        boolean      status;
        int          failedReindexAttempts = languageIds.size();
        Set<String>  searchableFields      = new HashSet<>();
        List<String> objectTypes           = operationMessage.getItemTypes();
        List<String> assignedSearchableFieldIds = operationMessage.getSearchableFieldIds();
        List<String> documentIds           = operationMessage.getAffectedItemIDs();
        
        // The message should not result in error if there is no document to re-index
        if(CollectionUtils.isEmpty(documentIds)) {
            logger.warn("{} received no documents for reindeixing", subscriberName);
            return true;
        }
        
        String       objectType            = objectTypes.get(0);
        intermediateDAO.clearObjectCache(objectType);
        Set<String>  searchableRefIds      = intermediateDAO.getSearchableReferences(objectType);
        Set<String>  searchableSubtableIDs = intermediateDAO.getSearchableSubtables(objectType);

        if(assignedSearchableFieldIds != null) {
            searchableRefIds.retainAll(assignedSearchableFieldIds);
            searchableSubtableIDs.retainAll(assignedSearchableFieldIds);
            assignedSearchableFieldIds.removeAll(searchableRefIds);
            assignedSearchableFieldIds.removeAll(searchableSubtableIDs);
            for (String field : assignedSearchableFieldIds){
                searchableFields.add(field + ":Value");
                searchableFields.add(field + ":FormattedValue");
            }
        } else {
            searchableFields = intermediateDAO.getSearchableAndStandardFields(objectType);
        }

        for (String refID : searchableRefIds) {
            searchableFields.add(ElasticsearchUtils.CONSTANT_REFERENCE + "." + refID + ".*");
        }

        for (String subtableID : searchableSubtableIDs) {
            searchableFields.add(ElasticsearchUtils.CONSTANT_SUBTABLE + "." + subtableID + ".*");
        }

        for (String languageId : languageIds) {
            status = reindexIndividualIndex(objectType ,languageId, documentIds, searchableFields);
            if(status)
            {
                failedReindexAttempts--;
            }
        }

        return (failedReindexAttempts == 0);
    }

    private boolean reindexIndividualIndex(String objectType, String languageId, List<String> documentIds, Set<String>  searchableFields)
    {
        boolean status          = false;
        String  sourceIndexName = handler.getElasticIndexName(objectType, languageId);
        String  targetIndexName = handler.getSearchableIndexName(objectType, languageId);
        try {
            int start = 0;
            int end = 0;
            
            while(end < documentIds.size()) {
                end += batchSize;
                if(end > documentIds.size()) {
                    end = documentIds.size();
                }
                
                logger.info("reindexing from {} to {} for batch {} to {}", sourceIndexName, targetIndexName, start, end);
                status = reindexQueryBuilder.reindexSearchableIndex(
                        sourceIndexName, 
                        targetIndexName, 
                        languageId, 
                        documentIds.subList(start, end), 
                        searchableFields);
                
                start = end;
            }
        } catch (IndexNotFoundException exception) {
            try {
                if (exception.getIndex().getName().equalsIgnoreCase(targetIndexName)) {
                    validateAndCreateIndex(objectType, targetIndexName);
                    return reindexIndividualIndex(objectType, languageId, documentIds, searchableFields);
                } else {
                    logger.info("Exception while Reindexing " + "SourceIndex [" + sourceIndexName + "] does not exist, so we are creating this index and trying again");
                    status = true;
                }
            } catch (Exception e) {
                logger.info("Exception while Reindexing unable to create index " + "DestIndex[" + targetIndexName + "] ");
            }
        } catch (CannotCreateConnectionException e) {
            ExceptionLogger.logError("[" + subscriberName + "]:" + " Failed to Reindex data as Cannot Create Connection Exception. " + " Error Message:", e, subscriberName);
        }

        return status;
    }

}