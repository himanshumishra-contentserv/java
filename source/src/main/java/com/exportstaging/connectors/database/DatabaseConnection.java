package com.exportstaging.connectors.database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.exportstaging.api.exception.ExportStagingException;

public interface DatabaseConnection {
    Session getSession() throws ExportStagingException;

    Cluster getCluster() throws ExportStagingException;

    String getKeyspace() throws ExportStagingException;

    void closeConnection();
}
