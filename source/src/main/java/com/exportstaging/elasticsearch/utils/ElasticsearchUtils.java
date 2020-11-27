package com.exportstaging.elasticsearch.utils;

public class ElasticsearchUtils
{


  ///// CONSTANTS //////////////////////////////////////////////////////////////////////////////////////////////////////


  /*
   * Constant to defaine fields inside the headers
   */
  public static final String ES_SEARCH_INDEX_SUFFIX      = "_search";
  public static final String ES_REINDEX_UPDATE_OPERATION = "Update";
  public static final String ES_REINDEX_DELETE_OPERATION = "Delete";
  public static final String ES_FIELDS_ID                = "ID";
  public static final String ES_FIELD_LANGUAGEID         = "LanguageID";
  public static final String ES_FIELD_ID                 = "ID";

  public static final String ES_CONSTANT_ITEM        = "Item";
  public static final String ES_CONSTANT_FILECONTENT = "FileContent";

  public static final String ES_SETTING_FIELD_LIMIT   = "index.mapping.total_fields.limit";
  public static final int    ES_DEFAULT_MAPPING_FIELD = 1000;

  // Constants to connect CS17.0 remote elastic host
  public static final String EXPORT_CONST_CS_ELASTIC_MODE          = "-mode";
  public static final String EXPORT_CS17_ELASTIC_SCHEME            = "http";
  public static final String EXPORT_CS17_ELASTIC_HOST              = "host";
  public static final String EXPORT_CS17_ELASTIC_PORT              = "port";
  public static final String EXPORT_CS17_ELASTIC_USER_NAME         = "userName";
  public static final String EXPORT_CS17_ELASTIC_PASSWORD          = "password";
  public static final String EXPORT_CS17_EXPORT_DB_PREFIX          = "export_";
  public static final String EXPORT_CS17_ELASTIC_DEFAULT_PORT      = "9200";
  public static final String EXPORT_CS17_ELASTIC_DEFAULT_USER_NAME = "";
  public static final String EXPORT_CS17_ELASTIC_DEFAULT_PASSWORD  = "";
  public static final String EXPORT_DELETE_OLD_INDEX               = "deleteOldIndex";
  public static final String EXPORT_DELETE_OLD_INDEX_VALUE         = "yes";
  public static final String EXPORT_SEARCHABLE_SUBSCRIBER_NAME     = "SearchableElasticSubscriber";
  public static final String CONSTANT_REFERENCE                    = "Reference";
  public static final String CONSTANT_SUBTABLE                     = "Subtable";
  public static final String EXPORT_CS_VERSION                     = "csversion";
  public static final String EXPORT_CS180_OLD_VERSION              = "cs18.0_old";

}
