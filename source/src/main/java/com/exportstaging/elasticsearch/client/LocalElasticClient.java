package com.exportstaging.elasticsearch.client;

import java.net.UnknownHostException;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component("LOCAL")
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
public class LocalElasticClient extends ElasticClient{

  @Override
  public RestHighLevelClient prepareClient() throws UnknownHostException {
    return new RestHighLevelClient(
        getRestClientBuilder(clusterAddress, httpPorts, isSsl)
        .setHttpClientConfigCallback(hcb -> {
          
          if(isSsl)
            hcb.setSSLContext(getDefaultSSLContext());

          CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(elasticUserName, elasticPassword));

          return hcb.setDefaultCredentialsProvider(credentialsProvider);
        })
        .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(
                    RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder
                    .setConnectTimeout(connectionTimeout)
                    .setSocketTimeout(socketTimeout);
            }
        }));
  }

}
