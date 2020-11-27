package com.exportstaging.api.implementation;

import com.exportstaging.api.CassandraAPI;
import com.exportstaging.api.dao.CassandraAPIDAOImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by CS58 on 02-02-2017.
 */

@Component
public class CassandraApiImpl implements CassandraAPI {
    @Autowired
    CassandraAPIDAOImpl cassandraAPIDAOImpl;

    @Override
    public boolean createIndex(String itemType, String subItemType, String fieldName) {
        return cassandraAPIDAOImpl.createIndex(itemType, subItemType, fieldName);
    }

    @Override
    public boolean dropIndex(String itemType, String subItemType, String fieldName) {
        return cassandraAPIDAOImpl.removeIndex(itemType, subItemType, fieldName);
    }

    public void close() {
        cassandraAPIDAOImpl.close();
    }
}
