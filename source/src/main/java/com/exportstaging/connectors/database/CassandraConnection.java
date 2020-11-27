package com.exportstaging.connectors.database;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.utils.CassandraListener;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Component("cassandraConnection")
public class CassandraConnection implements DatabaseConnection, DisposableBean {

    private Cluster cluster = null;
    private Session session = null;
    private int count = 0;

    @Value("${cassandra.connection.url}")
    private String connectionURL;
    @Value("${cassandra.keyspace}")
    private String keyspace;
    @Value("${export.tools.connection.retry}")
    private int retryCount;
    @Value("${export.tools.connection.delay}")
    private int retryDelay;
    @Value("${cassandra.username}")
    private String username;
    @Value("${cassandra.password}")
    private String password;

    private static String databaseType = null;

    @Autowired
    private CassandraListener listener;

    private CassandraConnection() {
    }

    private void createConnection() throws ExportStagingException {
        if (cluster == null || session == null || cluster.isClosed() || session.isClosed()) {
            setConfigurations();
            if (count < retryCount) {
                try {
                    if (count != 0) {
                        Thread.sleep(retryDelay);
                    } else {
                        String msg = "Trying to Connect to host name(s): " + connectionURL + " & keyspace name: " + keyspace;
                        System.out.println(msg);
                    }
                    cluster = Cluster.builder()
                            .withPoolingOptions(getConfiguredPoolOptions())
                            .addContactPointsWithPorts(getContactPoints(connectionURL))
                            .withReconnectionPolicy(new ConstantReconnectionPolicy(retryDelay))
                            .withCredentials(username, password)
                            .build().register(listener);
                    session = cluster.connect();

                    if (session != null && !session.isClosed()) {
                        String msg = "Connected to " + getDatabaseType();
                        System.out.println(msg);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception exception) {
                    session = null;
                    cluster = null;
                    count++;
                    createConnection();
                }
            } else {
                String msg = "ExportStaging Initialization Error: Connection to " + getDatabaseType()
                             + " was not successful. "
                             + "Please verify if " + getDatabaseType() + " is properly configured and running";
                throw new ExportStagingException(msg, ExportStagingException.CONNECTION_EXCEPTION);
            }
        }
    }

    /**
     * Method will set multiple contact points in case of cluster
     * If connection port number is not mentioned in the URL then default port of Cassandra should be considered
     * default port of Cassandra is 9042
     *
     * @param connectionURL String list of connection URL
     * @return will return object of InetSocketAddress which is useful to establish connection with Cassandra
     */
    private Collection<InetSocketAddress> getContactPoints(String connectionURL) throws UnknownHostException {
        Collection<InetSocketAddress> inetSocketAddresses = new ArrayList<>();
        Map<InetAddress, Integer> ipPortMapping = new HashMap();

        ipPortMapping = ExportMiscellaneousUtils.getURLDetails(connectionURL, ExportMiscellaneousUtils.DEFAULT_TCP_PORT_CASSANDRA);
        for (Map.Entry<InetAddress, Integer> ipPortDetails : ipPortMapping.entrySet()) {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(ipPortDetails.getKey(), ipPortDetails.getValue());
            inetSocketAddresses.add(inetSocketAddress);
        }

        return inetSocketAddresses;
    }

    public Session getSession() throws ExportStagingException {
        if (session == null || session.isClosed()) {
            createConnection();
        }
        return session;
    }

    public Cluster getCluster() throws ExportStagingException {
        if (cluster == null || cluster.isClosed()) {
            createConnection();
        }
        return cluster;
    }

    private void closeCluster() {
        cluster.close();
    }

    private void closeSession() {
        session.close();
    }

    public String getKeyspace() throws ExportStagingException {
        if (session == null || session.isClosed()) {
            createConnection();
        }
        return keyspace.toLowerCase();
    }

    @Override
    public void closeConnection() {
        if (session != null && !session.isClosed()) {
            closeSession();
            if (cluster != null && !cluster.isClosed()) {
                closeCluster();
            }
        }
    }

    private void setConfigurations() {
        if (System.getProperty("CassandraAddress") != null) {
            this.connectionURL = System.getProperty("CassandraAddress");
            this.keyspace = System.getProperty("CassandraKeyspace");
            this.username = System.getProperty("CassandraUserName");
            this.password = System.getProperty("CassandraPassword");
        }
    }

    private PoolingOptions getConfiguredPoolOptions() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setConnectionsPerHost(HostDistance.LOCAL, 4, 10)
                .setConnectionsPerHost(HostDistance.REMOTE, 2, 4);

        return poolingOptions;
    }


    /**
     * Provide the database type, Cassandra or ScyllaDB
     *
     * @return database type, Cassandra or ScyllaDB
     *
     * @throws ExportStagingException if exception occurs while getting the database type then it will be thrown
     */
    public String getDatabaseType() throws ExportStagingException
    {
        if (databaseType == null) {
            setDatabaseType();
        }
        return databaseType;
    }


    /**
     * Set the database type, Cassandra or ScyllaDB
     * We will check the system keyspece and will try to find scylla db specific table(scylla_local)
     * if this table is present then will set database type as ScyllaDB otherwise Cassandra will be set
     *
     * @throws ExportStagingException if exception occurs while checking the scylla specific table in system keyspace
     *                                then it will be thrown
     */
    public void setDatabaseType() throws ExportStagingException
    {
        TableMetadata table = getCluster().getMetadata()
                                          .getKeyspace(ExportMiscellaneousUtils.EXPORT_DB_SYSTEM)
                                          .getTable(ExportMiscellaneousUtils.EXPORT_TABLE_SCYLLA_LOCAL);
        databaseType = ((table == null)
                        ? ExportMiscellaneousUtils.EXPORT_DB_TYPE_CASSANDRA
                        : ExportMiscellaneousUtils.EXPORT_DB_TYPE_SCYLLA);
    }


    @Override
    public void destroy() throws Exception {
        closeConnection();
    }
}
