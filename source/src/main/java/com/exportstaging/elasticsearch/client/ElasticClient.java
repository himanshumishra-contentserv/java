package com.exportstaging.elasticsearch.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.eclipse.jetty.http.HttpScheme;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.springframework.beans.factory.annotation.Value;

import com.exportstaging.api.exception.CannotCreateConnectionException;

public abstract class ElasticClient {
  
  private final static Logger logger = Logger.getLogger("exportstaging");

  @Value("${elasticsearch.connection.retry}")
  protected int retryCount;
  @Value("${elasticsearch.connection.delay}")
  protected int retryDelay;
  @Value("${elasticsearch.address}")
  protected String clusterAddress;
  @Value("${elasticsearch.cluster}")
  protected String clusterExportStaging;
  @Value("${elasticsearch.username}")
  protected String elasticUserName;
  @Value("${elasticsearch.password}")
  protected String elasticPassword;
  @Value("${elasticsearch.http.port}")
  protected String httpPorts;
  @Value("${elasticsearch.ssl.enabled}")
  protected boolean isSsl;
  
  @Value("${elasticsearch.connection.timeout}")
  protected int connectionTimeout;
  @Value("${elasticsearch.socket.timeout}")
  protected int socketTimeout;

  private int count = 0;

  /**
   * creates elastic client with retries if specified
   * @param subscriberName
   * @return RestHighLevelClient
   * @throws CannotCreateConnectionException
   */
  public RestHighLevelClient createClient(String subscriberName) throws CannotCreateConnectionException
  {
    if (count < retryCount) {
      try {
        if (count != 0) {
          Thread.sleep(retryDelay);
        } else {
          logger.info("Trying to Connect to ElasticSearch for " + subscriberName 
              + " for hosts " + clusterAddress);
        }
        RestHighLevelClient client = prepareClient();
        client.ping(RequestOptions.DEFAULT);
        System.out.println("Connected to ElasticSearch");
        return client;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (UnknownHostException e) {
        System.err.println("ExportStaging Error: " + e.getMessage());
        System.exit(1);
      } catch (NoNodeAvailableException e) {
        count++;
        return createClient(subscriberName);
      } catch (IOException e) {
        System.err.println("ExportStaging Error: " + e.getMessage());
      }
    } 
    throw new CannotCreateConnectionException("Failed to connect to to ElasticSearch.");
  }
  
  /**
   * provide implementation for creating RestHighLevelClient object
   * @return
   * @throws UnknownHostException
   */
  protected abstract RestHighLevelClient prepareClient() throws UnknownHostException;
  
  /**
   * Get rest client builder with minimal configuration which can give a basic client
   * @param clusterAddress
   * @param httpPorts
   * @param isSSL
   * @return
   * @throws UnknownHostException
   */
  protected RestClientBuilder getRestClientBuilder(String clusterAddress, String httpPorts, boolean isSSL) 
      throws UnknownHostException {
    return RestClient.builder(getHosts(clusterAddress, httpPorts, isSSL));
  }
  
  /**
   * provided default SSL context for turning SSL verification off
   * @return SSLContext
   */
  protected SSLContext getDefaultSSLContext() {
    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, new TrustManager[] { new X509TrustManager() {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) 
            throws CertificateException {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) 
            throws CertificateException {}
      } }, new java.security.SecureRandom());
      
      return sc;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      logger.error("SSL Exception", e);
    }
    return null;
  }


  /**
   * Get elastic hosts
   * @return Map<InetAddress, Integer>
   * @throws UnknownHostException
   */
  protected HttpHost[] getHosts(String clusterAddress, String httpPorts, boolean isSSL) throws UnknownHostException {
    String[] hostList = clusterAddress.split(",");
    String[] ipList = httpPorts.split(",");

    if (hostList.length == ipList.length) {
      HttpHost[] hosts = new HttpHost[hostList.length];
      for (int i = 0; i < hostList.length; i++) {
        InetAddress inetAddress = InetAddress.getByName(hostList[i]);
        int port = Integer.parseInt(ipList[i].trim());
        hosts[i] = new HttpHost(inetAddress, port, 
            (isSSL ? HttpScheme.HTTPS : HttpScheme.HTTP).toString());
      }
      return hosts;
    } else {
      String infoMessage = "Please verify the host and port details. Count of host(s) and tcp port(s) mismatch...";
      logger.error(infoMessage);
      System.exit(1);
    }

    return new HttpHost[0];
  }
}
