package com.exportstaging.api.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * This Object holds the information of the Attribute Data for an Item
 */
public class ExportValues {

    private List<AttributeValue> attributeList = new ArrayList<>();
    private List<Reference> referenceList = new ArrayList<>();
    private List<Subtable> subtableList = new ArrayList<>();

    /**
     * Default Constructor for the Object
     */
    public ExportValues() {
    }

    /**
     * Overloaded Constructor
     *
     * @param attributeList List of {@link AttributeValue} Object for Simple Attributes
     * @param referenceList List of {@link Reference} Objects
     * @param subtableList  List of {@link Subtable} Objects
     */
    public ExportValues(List<AttributeValue> attributeList, List<Reference> referenceList, List<Subtable> subtableList) {
        this.attributeList = attributeList;
        this.referenceList = referenceList;
        this.subtableList = subtableList;
    }

    /**
     * Returns the List of {@link AttributeValue} Objects assigned to the Item
     *
     * @return List of {@link AttributeValue}
     */
    public List<AttributeValue> getAttributeList() {
        return attributeList;
    }

    /**
     * Assign the SimpleAttribute information to this Object
     *
     * @param attributeList List of {@link AttributeValue} for the Item in this object
     */
    public void setAttributeList(List<AttributeValue> attributeList) {
        this.attributeList = attributeList;
    }

    /**
     * Returns the list of {@link Reference} Objects assigned to the item
     *
     * @return List of {@link Reference}
     */
    public List<Reference> getReferenceList() {
        return referenceList;
    }

    /**
     * Assign reference information to the item
     *
     * @param referenceList List of {@link Reference} for the Item in this object
     */
    public void setReferenceList(List<Reference> referenceList) {
        this.referenceList = referenceList;
    }

    /**
     * Returns the List of {@link Subtable} Objects assigned to the item
     *
     * @return List of {@link Subtable}
     */
    public List<Subtable> getSubtableList() {
        return subtableList;
    }

    /**
     * Assign Subtable information to the item
     *
     * @param subtableList List of {@link Subtable} for the Item in this object
     */
    public void setSubtableList(List<Subtable> subtableList) {
        this.subtableList = subtableList;
    }

}