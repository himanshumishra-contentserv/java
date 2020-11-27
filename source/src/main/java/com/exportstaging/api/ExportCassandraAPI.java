package com.exportstaging.api;

import com.exportstaging.api.implementation.CassandraApiImpl;
import org.apache.xbean.spring.context.ClassPathXmlApplicationContext;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * The ExportCassandraAPI class use to perform cassandra modeling related operations.
 * This class implements the ApplicationContextAware interface to get the context.
 * When creating object must pass the itemType i.e Pdmarticle/Mamfile <i>ExternalItemAPI(String itemType)</i>
 * Following are the Item types used for which we can change the structure<br>
 * <b>ITEM_TYPE_PRODUCT : </b> For Pdmarticle<br>
 * <b>ITEM_TYPE_VIEW : </b> For Pdmarticlestructure<br>
 * <b>ITEM_TYPE_FILE : </b> For Mamfile<br>
 */
@Component
public class ExportCassandraAPI implements ApplicationContextAware {
    private static ClassPathXmlApplicationContext context = null;

    private static CassandraApiImpl cassandraImplApi;

    private String itemType = null;

    @Deprecated
    public ExportCassandraAPI() {
    }

    /**
     * constructor used to set Item type
     * According to ItemType index will be apply
     *
     * @param itemType String Type of an Item
     */
    public ExportCassandraAPI(String itemType) {
        this.itemType = itemType;
    }

    /**
     * Create index on a field of cassandra table
     * Index will be applied to the table which is specified in Item Type while creation of the object
     *
     * @param fieldName Name of a field on which index apply
     *
     * @return will return the status whether the index applied or not
     */
    boolean createIndex(String fieldName) {
        return cassandraImplApi.createIndex(this.itemType, null, fieldName);
    }

    /**
     * Index will be apply to field of SubItem type table
     * As of now only supporting Subtable & Reference.
     *
     * @param subItemType Name of SubItem
     * @param fieldName   Name of a field on which index apply
     *
     * @return will return the status whether the index applied or not
     */
    public boolean createIndex(String subItemType, String fieldName) {
        return cassandraImplApi.createIndex(this.itemType, subItemType, fieldName);
    }

    /**
     * Drop index of specified field
     *
     * @param fieldName Name of a field on which index apply
     *
     * @return will return the status whether the index dropped or not
     */
    public boolean dropIndex(String fieldName) {
        return cassandraImplApi.dropIndex(this.itemType, null, fieldName);
    }

    /**
     * Drop index of SubItemType cassandra table
     * As of now only supporting subtable & Reference.
     *
     * @param subItemType Name of SubItem
     * @param fieldName   Name of a field on which index apply
     *
     * @return will return the status whether the index dropped or not
     */
    public boolean dropIndex(String subItemType, String fieldName) {
        return cassandraImplApi.dropIndex(this.itemType, subItemType, fieldName);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        cassandraImplApi = (CassandraApiImpl) applicationContext.getBean("cassandraApiImpl");
    }
}
