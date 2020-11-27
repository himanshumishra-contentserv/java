package com.exportstaging.elasticsearch.clientbuilder;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.elasticsearch.client.ElasticClient;
import com.exportstaging.elasticsearch.client.LocalElasticClient;

/**
 * All client related services should be provided by this class
 */
@Component
public class ElasticClientBuilder implements ClientBuilder
{

  ///// INSTANCE VARS //////////////////////////////////////////////////////////////////////////////////////////////////

  @Autowired
  private ApplicationContext appContext;
  
  private final static Logger logger = Logger.getLogger("exportstaging");
  
  private static ConcurrentHashMap<String, RestHighLevelClient> clients = new ConcurrentHashMap<>();


  ///// PUBLIC METHODS /////////////////////////////////////////////////////////////////////////////////////////////////  
  
  /**
   * Method will return client object to communicate with elastic search
   *
   * @return Client object of elastic search
   */
  public RestHighLevelClient getClient(String subscriberName) throws CannotCreateConnectionException
  {
    if (clients.get(subscriberName) == null) {
      ElasticClient client = (ElasticClient)appContext.getBean(LocalElasticClient.class);
      clients.put(subscriberName, client.createClient(subscriberName));
    }

    return clients.get(subscriberName);
  }


  /**
   * provides admin admin from the connected client
   *
   * @return object of AdminClient from which indices admin client can be get for query execution
   *
   * @throws CannotCreateConnectionException throws exception if not able to connect to elastic client
   */
  public RestClient getAdmin(String subscriberName) throws CannotCreateConnectionException
  {
    return getClient(subscriberName).getLowLevelClient();
  }


  /**
   * provides IndicesAdminClient to execute request
   *
   * @return object of IndicesAdminClient
   *
   * @throws CannotCreateConnectionException throws exception if not able to connect to elastic client
   */
  public IndicesClient getIndicesAdminClient(String subscriberName) throws CannotCreateConnectionException
  {
    return getClient(subscriberName).indices();
  }


  /**
   * Method will close the elastic search client connection
   */
  public void closeClient()
  {
    for(Entry<String, RestHighLevelClient> client : clients.entrySet()) {
      try {
        client.getValue().close();
      } catch (IOException e) {
        logger.error("Exception while closing client for subscriber " + client.getKey(), e);
      }
    }
  }
  
}
