package com.exportstaging.elasticsearch.updatedatamodel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.connectors.idbconnector.IntermediateDAO;
import com.exportstaging.elasticsearch.ElasticOperationHandler;
import com.exportstaging.elasticsearch.ElasticSearchOperations;
import com.exportstaging.elasticsearch.querybuilder.ReindexQueryBuilder;
import com.exportstaging.elasticsearch.utils.ElasticsearchUtils;
import com.exportstaging.subscribers.ElasticSubscriber;

/**
 * The responsibility of the class is to create indices for elastic 6.3
 */
@Component
public class ElasticIndexUpdater
{


  ///// INSTANCE VARS //////////////////////////////////////////////////////////////////////////////////////////////////


  @Autowired
  private ElasticSubscriber       elasticSubscriber;
  @Autowired
  private ElasticSearchOperations searchOperations;
  @Autowired
  @Qualifier("elasticReindexQueryBuilder")
  private ReindexQueryBuilder     queryBuilder;
  @Autowired
  private ElasticOperationHandler handler;
  @Autowired
  private IntermediateDAO         intermediateDAO;

  private Map<String, String> cs17ElasticCredentials;
  private Set<String> cs17ElasticMandetoryParams;
  private boolean isCS18OldVersion;
  private boolean isDeleteOldIndexes;

  protected final Logger logger = Logger.getLogger("exportstaging");

  @Value("${core.project.name}")
  private String projectName;


  ///// PUBLIC METHODS /////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * API will be responsible to create the indices for elastic search as per elasticsearch 6.3 version
   * After indices creation, user needs to transfer the data from old(elastic 2.4) version to newly created indices
   * by the script
   *
   * @param startUpSettings boolean to identify the provided CS version is CS18.0 old or CS17.0 new
   */
  public void updateIndex(String[] startUpSettings)
  {
    try {
      List<String> param = Arrays.asList(startUpSettings);
      setIsCS18OldVersion(param);
      setIsDeleteOldIndexes(param);
      boolean isCS18OldVersion = getIsCS18OldVersion();
      if (!isCS18OldVersion) {
        setCs17ElasticMandetoryParams();
        setCS17ElasticCredentials(param);
      }
      updateIndexPerLanguageFlag();
      List<String> coreItemType              = ExportMiscellaneousUtils.getConfiguredTypes();
      List<String> elasticSupportedItemTypes = handler.getElasticSupportedItemTypes(coreItemType);
      handler.avoidRecordProcessing(elasticSupportedItemTypes);
      handler.avoidMamfileContentType(elasticSupportedItemTypes);
      if (!elasticSupportedItemTypes.isEmpty()) {
        //This creates main indices
        createEmptyIndices(elasticSupportedItemTypes, elasticSubscriber.getSubscriberName());
        //This creates searchable subscriber indices
        createEmptyIndices(elasticSupportedItemTypes, ElasticsearchUtils.EXPORT_SEARCHABLE_SUBSCRIBER_NAME);
        for (String objectType : elasticSupportedItemTypes) {
          processMainIndex(objectType);
          processSearchableIndex(objectType);
          if (getIsDeleteOldIndexes()) {
            processDeleteOldIndex(objectType);
          }
        }
      }
      else {
        System.out.println("No Object Types Configured for Elasticsearch");
      }
    } catch (Exception exception) {
      System.out.println(exception.getMessage());
      logger.error("[ElasticIndexUpdater:updateIndex()] Exception:" + exception.getMessage());
    }
  }

    /**
     * Setting the CS version
     *
     * @param param List Params provided by user
     */
    public void setIsCS18OldVersion(List<String> param) {
        isCS18OldVersion = param.contains(
                ElasticsearchUtils.EXPORT_CS_VERSION + ":" + ElasticsearchUtils.EXPORT_CS180_OLD_VERSION);
    }


  ///// PRIVATE METHODS ////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Main and searchable indices will be created if its not there
   *
   * @param coreItemType String type of an object
   */
  private void createEmptyIndices(List<String> coreItemType, String subscriberName)
  {
    try {
      elasticSubscriber.initializeSearchEngine(coreItemType, subscriberName);
    } catch (CannotCreateConnectionException exception) {
      System.out.println(
        "Could not able to establish connection with elasticsearch possible cuase :" + exception.getMessage());
    } catch (Exception exception) {
      System.out.println(exception.getMessage());
      logger.error("[ELasticIdexUpdater:createEmptyIndices()] Exception:" + exception.getMessage());
    }
  }


    /**
     * Set the Cs17 Elastic credentials into key-value pairs
     *
     * @param params CS17 Elastic credentials provided by User
     */
    private void setCS17ElasticCredentials(List<String> params) {
        try {
            cs17ElasticCredentials = new HashMap<>();
            String[] paramKeyValue;
            for (String param : params) {
                if (param.contains(":")) {
                    paramKeyValue = param.split(":");
                    cs17ElasticCredentials.put(paramKeyValue[0], paramKeyValue[1]);
                }
            }
            validateStartUpParams();
            updateOptionalParams();
        } catch (ExportStagingException exception) {
            String errorMessage = exception.getMessage();
            System.out.println(errorMessage);
            handler.logError(errorMessage, exception);
            System.exit(0);
        } catch (Exception exception) {
            String errorMessage = exception.getMessage();
            System.out.println(errorMessage);
            handler.logError(errorMessage, exception);
            System.exit(0);
        }
    }


    /**
     * Validate the start up params
     *
     * @throws ExportStagingException Throws if any mandatory param is not provided
     */
    private void validateStartUpParams() throws ExportStagingException {
        Map<String, String> cs170ElasticCredentials = getCs17ElasticCredentials();
        Set<String> elasticMandetoryParams = getCs17ElasticMendetoryParams();
        Set<String> elasticParamKeys = cs170ElasticCredentials.keySet();
        if (!elasticParamKeys.containsAll(elasticMandetoryParams)) {
            throw new ExportStagingException("Please provide the mandatory parameter, " +
                    "e.g: Project Name, -mode, csversion and host must be provided by user");
        }
    }


    /**
     * Update the optional param values, if user has not provided
     */
    private void updateOptionalParams() {
        Map<String, String> cs170ElasticCredentials = getCs17ElasticCredentials();
        if (!cs170ElasticCredentials.containsKey(ElasticsearchUtils.EXPORT_CS17_ELASTIC_PORT))
            cs170ElasticCredentials.put(ElasticsearchUtils.EXPORT_CS17_ELASTIC_PORT,
                    ElasticsearchUtils.EXPORT_CS17_ELASTIC_DEFAULT_PORT);

        if (!cs170ElasticCredentials.containsKey(ElasticsearchUtils.EXPORT_CS17_ELASTIC_USER_NAME))
            cs170ElasticCredentials.put(ElasticsearchUtils.EXPORT_CS17_ELASTIC_USER_NAME,
                    ElasticsearchUtils.EXPORT_CS17_ELASTIC_DEFAULT_USER_NAME);

        if (!cs170ElasticCredentials.containsKey(ElasticsearchUtils.EXPORT_CS17_ELASTIC_PASSWORD))
            cs170ElasticCredentials.put(ElasticsearchUtils.EXPORT_CS17_ELASTIC_PASSWORD,
                    ElasticsearchUtils.EXPORT_CS17_ELASTIC_DEFAULT_PASSWORD);
    }


    /**
   * Reindexing of searchable index will be done here
   *
   * @param objectType String object type for which reindexing needs to be done
   */
  private void processSearchableIndex(String objectType)
  {
    List<String> languageIds = intermediateDAO.getLanguageIds();
    Set<String> searchableFields = new HashSet<>();
    boolean isCS18OldVersion = getIsCS18OldVersion();
    searchableFields.addAll(handler.getSearchableFields(objectType, true));
    searchableFields.addAll(handler.getSearchableReferenceFields(objectType));
    searchableFields.addAll(handler.getSearchableSubtableFields(objectType));

    for (String languageId : languageIds) {
      String sourceIndexName;
      String targetIndexName = handler.getSearchableIndexName(objectType, languageId);
      sourceIndexName = isCS18OldVersion
                        ? handler.getElasticIndexName(objectType, languageId)
                        : ElasticsearchUtils.EXPORT_CS17_EXPORT_DB_PREFIX + projectName.toLowerCase() + "_" +
                          objectType
                          .toLowerCase() + "_" + languageId;
      queryBuilder.reindexSearchableIndex(sourceIndexName, targetIndexName, searchableFields.toArray(new String[0]));
    }
  }


  /**
   * In Case of migration from CS18.0 old model to new model, need to create new indices as per language id
   * Newly created indices(export_cslive_pdmarticle_1,..) will be fill here
   *
   * @param objectType String type of an item
   */
  private void processMainIndex(String objectType)
  {
    List<String> languageIds = intermediateDAO.getLanguageIds();
    boolean isCS18OldVersion = getIsCS18OldVersion();
    for (String languageId : languageIds) {
      String sourceIndexName;
      String targetIndexName = handler.getElasticIndexName(objectType, languageId);
      sourceIndexName = isCS18OldVersion
                        ? handler.getElasticIndexName(objectType)
                        : ElasticsearchUtils.EXPORT_CS17_EXPORT_DB_PREFIX + projectName.toLowerCase() + "_" +
                          objectType
                          .toLowerCase() + "_" + languageId;
      queryBuilder.reindexMainIndex(sourceIndexName, targetIndexName, languageId);
    }
  }


  /**
   * Delete old indexes of provided Object type
   *
   * @param objectType Type of the Object
   *
   * @throws CannotCreateConnectionException
   */
  private void processDeleteOldIndex(String objectType) throws CannotCreateConnectionException
  {
    String sourceIndexName = null;
    boolean isCS18OldVersion = getIsCS18OldVersion();
    if (isCS18OldVersion) {
      sourceIndexName = handler.getElasticIndexName(objectType);
      queryBuilder.deleteOldIndex(sourceIndexName);
    }
    else {
      List<String> languageIds = intermediateDAO.getLanguageIds();
      for (String languageId : languageIds) {
        sourceIndexName = ElasticsearchUtils.EXPORT_CS17_EXPORT_DB_PREFIX + projectName.toLowerCase() + "_" + objectType
          .toLowerCase() + "_" + languageId;
        queryBuilder.deleteOldIndex(sourceIndexName);
      }
    }
  }


  /**
   * To create indices with new approach, this flag needs to be set
   */
  private void updateIndexPerLanguageFlag()
  {
    handler.setLanguagePerIndexFlag(true);
  }


    /**
     * Gives CS17.0 Elastic credentials
     *
     * @return CS17.0 Elastic Credentials
     */
    public Map<String, String> getCs17ElasticCredentials() {
        return cs17ElasticCredentials;
    }


    /**
     * Returns true, if migration from CS18.0_old. False, if migration from CS17.0_new
     *
     * @return true, if migration from CS18.0_old. False, if migration from CS17.0_new
     */
    public boolean getIsCS18OldVersion() {
        return isCS18OldVersion;
    }


    /**
     * Gives mandatory params for CS17.0 Elastic
     *
     * @return Map of Madetory Params
     */
    public Set<String> getCs17ElasticMendetoryParams() {
        return cs17ElasticMandetoryParams;
    }


    /**
     * Setting the CS17.0 Elastic mandatory params
     */
    public void setCs17ElasticMandetoryParams() {
        cs17ElasticMandetoryParams = new HashSet<>();
        cs17ElasticMandetoryParams.add(ElasticsearchUtils.EXPORT_CONST_CS_ELASTIC_MODE);
        cs17ElasticMandetoryParams.add(ElasticsearchUtils.EXPORT_CS_VERSION);
        cs17ElasticMandetoryParams.add(ElasticsearchUtils.EXPORT_CS17_ELASTIC_HOST);
    }


    /**
     * Status of the deleting old indexes on the basis of user provided param
     *
     * @return True, if user wants to delete index after migration, otherwise false
     */
    public boolean getIsDeleteOldIndexes() {
        return isDeleteOldIndexes;
    }


    /**
     * Setting the status for deleting old indexes
     *
     * @param param List User provided params
     */
    public void setIsDeleteOldIndexes(List<String> param) {
        isDeleteOldIndexes = param.contains(ElasticsearchUtils.EXPORT_DELETE_OLD_INDEX + ":" +
                ElasticsearchUtils.EXPORT_DELETE_OLD_INDEX_VALUE);
    }
}
