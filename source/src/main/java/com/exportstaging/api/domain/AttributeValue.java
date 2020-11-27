package com.exportstaging.api.domain;


/**
 * Returned by getValues(). Contains information about simple attributes.
 */
public class AttributeValue {
    private String id;
    private int languageID;
    private String value;
    private String formattedValue;

    /**
     * Default Constructor to create the AttributeValue object
     */
    public AttributeValue() {
    }

    /**
     * Constructor to create and fill the AttributeValue Object
     *
     * @param id             ID of the Attribute
     * @param languageID     Language ID of the attribute
     * @param value          Database value of the Attribute
     * @param formattedValue Value visible on UI for the Attribute
     */
    public AttributeValue(String id, int languageID, String value, String formattedValue) {
        this.id = id;
        this.languageID = languageID;
        this.value = value;
        this.formattedValue = formattedValue;
    }

    /**
     * This method gives the ID of the Attribute Object
     *
     * @return Id of the attribute
     */
    public String getId() {
        return id;
    }

    /**
     * This method sets the ID to the Attribute Object
     *
     * @param id ID value to be set for object
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * This method gives the LanguageID of the Attribute Object
     *
     * @return LanguageID of the attribute
     */
    public int getLanguageID() {
        return languageID;
    }

    /**
     * This method sets the LanguageID to the Attribute Object
     *
     * @param languageID LanguageID value to be set for object
     */
    public void setLanguageID(int languageID) {
        this.languageID = languageID;
    }

    /**
     * This method gives the Database Value of the Attribute Object
     *
     * @return Value of the attribute
     */
    public String getValue() {
        return value;
    }

    /**
     * This method sets the Value to the Attribute Object
     *
     * @param value Value of the attribute
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * This method gives the Value visible on UI of the Attribute Object
     *
     * @return Formatted value of the attribute
     */
    public String getFormattedValue() {
        return formattedValue;
    }

    /**
     * This method sets the FormattedValue to the Attribute Object
     *
     * @param formattedValue FormattedValue of the attribute
     */
    public void setFormattedValue(String formattedValue) {
        this.formattedValue = formattedValue;
    }

}
