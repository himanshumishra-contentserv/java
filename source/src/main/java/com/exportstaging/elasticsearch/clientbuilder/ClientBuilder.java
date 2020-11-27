package com.exportstaging.elasticsearch.clientbuilder;

import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import com.exportstaging.api.exception.CannotCreateConnectionException;

public interface ClientBuilder
{
  /**
   * Method will return client object to communicate with elastic search
   *
   * @return Client object of elastic search
   */
  RestHighLevelClient getClient(String subscriberName) throws CannotCreateConnectionException;
  
  /**
   * provides admin admin from the connected client
   *
   * @return object of AdminClient from which indices admin client can be get for query execution
   *
   * @throws CannotCreateConnectionException throws exception if not able to connect to elastic client
   */
  RestClient getAdmin(String subscriberName) throws CannotCreateConnectionException;

  /**
   * provides IndicesAdminClient to execute request
   *
   * @return object of IndicesAdminClient
   *
   * @throws CannotCreateConnectionException throws exception if not able to connect to elastic client
   */
  IndicesClient getIndicesAdminClient(String subscriberName) throws CannotCreateConnectionException;

  /**
   * Method will close the elastic search client connection
   */
  void closeClient();
}
