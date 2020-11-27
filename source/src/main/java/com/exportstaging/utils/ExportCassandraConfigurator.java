package com.exportstaging.utils;

/**
 * This class manage the information about cassandra address and keyspace name.
 * Default cassandra configuration while using export database External API is url as localhost and the keyspace name as export_
 * Create the object of this class with require parameter to change the cassandra configuration as per requirement
 */
public class ExportCassandraConfigurator {

    /**
     * Configure the cassandra url and keyspace name
     *
     * @param address  Url address
     * @param keyspace Keyspace name
     */
    public ExportCassandraConfigurator(String address, String keyspace) {
        setCassandraProperties(address, keyspace);
    }

    public void setCassandraUsername(String userName) {
        System.setProperty("CassandraUserName", userName);
    }

    public void setCassandraPassword(String password) {
        System.setProperty("CassandraPassword", password);
    }

    private void setCassandraProperties(String address, String keyspace) {
        System.setProperty("CassandraAddress", address);
        System.setProperty("CassandraKeyspace", keyspace);
    }

    /**
     * Method set the cassandra connection url
     *
     * @param address Url address on which cassandra configured
     */
    public void setCassandraAddress(String address) {
        System.setProperty("CassandraAddress", address);
    }

    /**
     * Returns configured url address of cassandra.
     *
     * @return Cassandra url address
     */
    public String getCassandraAddress() {
        return System.getProperty("CassandraAddress");
    }

    /**
     * Method set the cassandra connection keyspace name
     *
     * @param keyspace Keyspace name on which data is exported
     */
    public void setCassandraKeyspace(String keyspace) {
        System.setProperty("CassandraKeyspace", keyspace);
    }

    /**
     * Method returns the configured keyspace name
     *
     * @return Name of the keyspace on which data is exported
     */
    public String getCassandraKeyspace() {
        return System.getProperty("CassandraKeyspace");
    }
}
