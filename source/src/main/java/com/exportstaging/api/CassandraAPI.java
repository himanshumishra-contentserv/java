package com.exportstaging.api;

/**
 * This interface contain all Cassandra modeling related operations.
 * The methods in this interface can be used to define modeling of cassandra database
 * Following are the Item types for which we can change the structure<br>
 * <b>ITEM_TYPE_PRODUCT : </b> For Pdmarticle<br>
 * <b>ITEM_TYPE_VIEW : </b> For Pdmarticlestructure<br>
 * <b>ITEM_TYPE_FILE : </b> For Mamfile<br>
 */
public interface CassandraAPI {
    String ITEM_TYPE_PRODUCT = "Pdmarticle";
    String ITEM_TYPE_VIEW = "Pdmarticlestructure";
    String ITEM_TYPE_FILE = "Mamfile";
    String ITEM_TYPE_REFERENCE = "Reference";
    String ITEM_TYPE_SUBTABLE = "Subtable";

    /**
     * Create index on field of a given table
     * Table name would be created using subItemType
     * As of now supported only for pdmarticle, pdmarticlestructure, mamfile, subtable and reference
     *
     * @param itemType    Name of an Item
     * @param subItemType Name of SubItem
     * @param fieldName   Name of a field on which index apply
     * @return will return the status whether the index applied or not
     */
    boolean createIndex(String itemType, String subItemType, String fieldName);

    /**
     * Drop  index on field of a given table
     * Table name would be created using subItemType
     * As of now supported only for pdmarticle, pdmarticlestructure, mamfile, subtable and reference
     *
     * @param itemType    Name of an Item
     * @param subItemType Name of SubItem
     * @param fieldName   Name of a field on which index apply
     * @return will return the status whether the index dropped or not
     */
    boolean dropIndex(String itemType, String subItemType, String fieldName);

}
