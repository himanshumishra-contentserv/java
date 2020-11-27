package com.exportstaging.api.searchfilter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * SearchFilterCriteria contain different type of search filters which are
 * supported in filter criteria.
 */
public class SearchFilterCriteria {


    public final static String SEARCH_FILTER_BY_ITEM_ID = "\"ID\"";
    public final static String SEARCH_FILTER_BY_WORKFLOW_ID = "\"WorkflowID\"";
    public final static String SEARCH_FILTER_BY_STATE_ID = "\"StateID\"";
    public final static String SEARCH_FILTER_BY_CLASS_MAPPING = "\"ClassMapping\"";
    public final static String SEARCH_FILTER_BY_PARENTS = "\"_Parents\"";
    public final static String SEARCH_FILTER_BY_PARENT_ID = "\"ParentID\"";
    public final static String SEARCH_FILTER_BY_CHANGED_AT = "\"LastChange\"";
    public final static String SEARCH_FILTER_BY_LANGUAGE_SHORTNAME = "\"LanguageShortName\"";
    public final static String SEARCH_FILTER_BY_LANGUAGE_ID = "\"LanguageID\"";
    public final static String SEARCH_FILTER_BY_LABEL = "\"Label\"";
    public final static String SEARCH_FILTER_BY_IS_FOLDER = "\"IsFolder\"";
    public final static String SEARCH_FILTER_BY_EXTERNAL_KEY = "\"ExternalKey\"";
    public final static String SEARCH_FILTER_BY_CREATION_DATE = "\"CreationDate\"";
    public final static String SEARCH_FILTER_BY_COPY_OF = "\"CopyOf\"";
    public final static String SEARCH_FILTER_BY_CHECKOUT_USER = "\"CheckoutUser\"";
    public final static String SEARCH_FILTER_BY_AUTHOR_ID = "\"AuthorID\"";
    public final static String SEARCH_FILTER_BY_AUTHOR = "\"Author\"";
    public final static String SEARCH_FILTER_BY_FORMATTEDVALUE = ":FormattedValue\"";
    // public final static String OR_OPERATOR = ", OR , ";

    public final static String AND_OPERATOR = ", AND , ";
    public final static String EQUAL_OPERATOR = " =  ";
    public final static String LESSTHAN_OPERATOR = " <  ";
    public final static String GREATERTHAN_OPERATOR = " >  ";
    public final static String LIKE_OPERATOR = " LIKE  ";
    public final static String CONTAINS_OPERATOR = " CONTAINS  ";
    public final static String OPERATOR_IN = " IN ";


    private String itemId;
    private Set<Long> itemIds;
    private String workflowId;
    private String stateId;
    private String classID;
    private String parents;
    private String parentId;
    private String lastChange;
    private String languageShortName;
    private String languageShortNameValue;
    private String languageId;
    private String label;
    private String isFolder;
    private String externalKey;
    private String creationDate;
    private String copyOf;
    private String checkoutUser;
    private String authorId;
    private String author;
    private String customAttribute = "";
    private String userDefineFilter;
    private boolean language;

    /**
     * Default Constructor
     */
    public SearchFilterCriteria() {

    }

    /**
     * Set the ItemIDs for filtering {@link SearchFilterCriteria}
     *
     * @param itemIds List of item ids
     */
    public void setItemIds(Set<Long> itemIds) {
        if (this.itemIds == null)
            this.itemIds = itemIds;
        else
            this.itemIds.addAll(itemIds);
    }

    /**
     * Returns the list of item ids which is set for filtering {@link SearchFilterCriteria}
     *
     * @return List of item ids
     */
    public Set<Long> getItemIds() {
        return itemIds;
    }

    /**
     * Returns ItemID for the current item
     *
     * @return ItemID of the item
     */
    public String getItemId() {
        return itemId;
    }

    /**
     * Set the ItemID for filtering {@link SearchFilterCriteria}
     *
     * @param itemId ItemID
     */
    public void setItemId(String itemId) {
        this.itemId = itemId;
        if (itemIds == null) {
            Set<Long> itemSet = new HashSet<>();
            itemSet.add(Long.parseLong(itemId));
            itemIds = itemSet;
        } else {
            itemIds.add(Long.parseLong(itemId));
        }
    }

    /**
     * Set the ItemID for filtering {@link SearchFilterCriteria}
     *
     * @param itemId Object ID to fetch data.
     */
    public void setItemId(long itemId) {
        this.itemId = String.valueOf(itemId);
        if (itemIds == null) {
            Set<Long> itemSet = new HashSet<>();
            itemSet.add(itemId);
            itemIds = itemSet;
        } else {
            itemIds.add(itemId);
        }
    }

    private String getItemIdsFilter() {
        return SEARCH_FILTER_BY_ITEM_ID + OPERATOR_IN + "(" + StringUtils.join(itemIds, ',') + ")";
    }

    /**
     * Create Search Filter using Workflow ID Value and Return.
     *
     * @return String: Return Search Filter using this Workflow ID Column on
     * Equality(=) basis.
     */
    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = SEARCH_FILTER_BY_WORKFLOW_ID + " = " + workflowId;
    }

    public void setWorkflowId(String workflowId, String operator) {
        this.workflowId = SEARCH_FILTER_BY_WORKFLOW_ID + operator + " " + workflowId;
    }

    /**
     * Create Search Filter using State ID Value and Return.
     *
     * @return String: Return Search Filter using this State ID Column on
     * Equality(=) basis.
     */
    public String getStateId() {
        return stateId;
    }

    public void setStateId(String stateId) {
        this.stateId = SEARCH_FILTER_BY_STATE_ID + " = " + stateId;
    }

    public void setStateId(String stateId, String operator) {
        this.stateId = SEARCH_FILTER_BY_STATE_ID + " " + operator + " " + stateId;
    }

    /**
     * Create Search Filter using ClassMapping Value and Return.
     *
     * @return String: Return Search Filter using this ClassMapping
     * Column on LIKE basis.
     */
    public String getClassID() {
        return classID;
    }

    public void setClassID(String classID) {
        this.classID = SEARCH_FILTER_BY_CLASS_MAPPING + LIKE_OPERATOR + "'% " + classID + " %'";
    }

    /**
     * To Set the Parent
     * Always internally use LIKE operator.
     *
     * @param parents string Parents separated before and after space
     */
    public void setParents(String parents) {
        this.parents = SEARCH_FILTER_BY_PARENTS + LIKE_OPERATOR + "'% " + parents + " %'";
    }


    public String getParents() {
        return parents;
    }

    /**
     * Create Search Filter using Parent ID Value and Return.
     *
     * @return String: Return Search Filter using this Parent ID Column on
     * Equality(=) basis.
     */
    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = SEARCH_FILTER_BY_PARENT_ID + " = " + parentId;
    }

    public void setParentId(String parentId, String operator) {
        this.parentId = SEARCH_FILTER_BY_PARENT_ID + " " + operator + " " + parentId;
    }

    /**
     * Create Search Filter using Last Change Value and Return.
     *
     * @return String: Return Search Filter using this Last Change Column on
     * Equality(=) basis.
     */
    public String getLastChange() {
        return lastChange;
    }

    public void setLastChange(String lastChange) {
        this.lastChange = SEARCH_FILTER_BY_CHANGED_AT + " = " + "'" + lastChange + "'";
    }

    public void setLastChange(String lastChange, String operator) {
        this.lastChange = SEARCH_FILTER_BY_CHANGED_AT + operator + "'" + lastChange + "'";
    }

    /**
     * Create Search Filter using ShortNameLanguage Value and Return.
     *
     * @return String: Return Search Filter using this ShortNameLanguage Column
     * on Equality(=) basis.
     */
    public String getLanguageShortName() {
        return languageShortName;
    }

    public void setLanguageShortName(String languageShortName) {
        this.languageShortName = SEARCH_FILTER_BY_LANGUAGE_SHORTNAME + " = " + "'" + languageShortName + "'";
        this.languageShortNameValue = languageShortName;
    }

    public String getLanguageShortNameValue() {
        return this.languageShortNameValue;
    }

    public void setLanguageShortNameValue(String languageShortNameValue) {
        this.languageShortNameValue = languageShortNameValue;
    }

    /**
     * Create Search Filter using Language ID Value and Return.
     *
     * @return String: Return Search Filter using this Language ID Column on
     * Equality(=) basis.
     */

    public String getLanguageId() {
        return languageId;
    }

    public void setLanguageId(String languageId) {
        this.languageId = SEARCH_FILTER_BY_LANGUAGE_ID + " = " + languageId;
    }

    /**
     * Create Search Filter using Label Value and Return.
     *
     * @return String: Return Search Filter using this Label Column on
     * Equality(=) basis.
     */
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = SEARCH_FILTER_BY_LABEL + " = " + "'" + label + "'";
    }

    /**
     * Create Search Filter using IsFolder Value and Return.
     *
     * @return String: Return Search Filter using this IsFolder Column on
     * Equality(=) basis.
     */
    public String getIsFolder() {
        return isFolder;
    }

    public void setIsFolder(String isFolder) {
        this.isFolder = SEARCH_FILTER_BY_IS_FOLDER + " = " + isFolder;
    }

    /**
     * Create Search Filter using External Key Value and Return.
     *
     * @return String: Return Search Filter using this External Key Column on
     * Equality(=) basis.
     */
    public String getExternalKey() {
        return externalKey;
    }

    public void setExternalKey(String externalKey) {
        this.externalKey = SEARCH_FILTER_BY_EXTERNAL_KEY + " = " + "'" + externalKey + "'";
    }

    /**
     * Create Search Filter using Creation Date Value and Return.
     *
     * @return String: Return Search Filter using this Creation Date Column on
     * Equality(=) basis.
     */
    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = SEARCH_FILTER_BY_CREATION_DATE + " = " + "'" + creationDate + "'";
    }

    public void setCreationDate(String creationDate, String operator) {
        this.creationDate = SEARCH_FILTER_BY_CREATION_DATE + operator + "'" + creationDate + "'";
    }

    /**
     * Create Search Filter using CopyOf Value and Return.
     *
     * @return String: Return Search Filter using this CopyOf Column on
     * Equality(=) basis.
     */
    public String getCopyOf() {
        return copyOf;
    }

    public void setCopyOf(String copyOf) {
        this.copyOf = SEARCH_FILTER_BY_COPY_OF + " = " + copyOf;
    }

    /**
     * Create Search Filter using Checkout User Value and Return.
     *
     * @return String: Return Search Filter using this Checkout User Column on
     * Equality(=) basis.
     */
    public String getCheckoutUser() {
        return checkoutUser;
    }

    public void setCheckoutUser(String checkoutUser) {
        this.checkoutUser = SEARCH_FILTER_BY_CHECKOUT_USER + " = " + checkoutUser;
    }

    /**
     * Create Search Filter using Author ID Value and Return.
     *
     * @return String: Return Search Filter using this Author ID Column on
     * Equality(=) basis.
     */
    public String getAuthorId() {
        return authorId;
    }

    public void setAuthorId(String authorId) {
        this.authorId = SEARCH_FILTER_BY_AUTHOR_ID + " = " + authorId;
    }

    /**
     * Create Search Filter using Author Value and Return.
     *
     * @return String: Return Search Filter using this Author Column on
     * Equality(=) basis.
     */
    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = SEARCH_FILTER_BY_AUTHOR + " = " + "'" + author + "'";
    }

    public boolean getLanguage() {
        return language;
    }

    public void setLanguage(boolean language) {
        this.language = language;
    }

    public void setCustomAttribute(String attributeID, String FormattedValue) {
        this.customAttribute = "\"" + attributeID + SEARCH_FILTER_BY_FORMATTEDVALUE + " = " + "'" + FormattedValue + "'";
    }

    public void setUserDefineFilter(String userDefineFilter) {
        this.userDefineFilter = userDefineFilter;
    }

    public String getUserDefineFilter() {
        return userDefineFilter;
    }

    public String getCustomAttribute() {
        return this.customAttribute;
    }

    @Override
    public String toString() {
        return "SearchFilterCriteria [itemIds=" + itemIds + ", workflowId=" + workflowId + ", stateId=" + stateId
                + ", classID=" + classID + ", parentId=" + parentId
                + ", lastChange=" + lastChange + ", languageShortName=" + languageShortName
                + ", languageShortNameValue=" + languageShortNameValue + ", languageId=" + languageId + ", label="
                + label + ", isFolder=" + isFolder + ", externalKey=" + externalKey + ", creationDate=" + creationDate
                + ", copyOf=" + copyOf + ", checkoutUser=" + checkoutUser + ", authorId=" + authorId + ", author="
                + author + ", language=" + language + "]";
    }

    @JsonIgnore
    public String getANDOperator() {
        return AND_OPERATOR;
    }

    /**
     * Get all the Filters and Combine with "AND" operator.
     *
     * @return :String Return Search Filters using Given Columns on Equality(=)
     * basis.
     */
    @JsonIgnore
    public String getFilters() {
        String filters = "";
        if (itemIds != null)
            filters = filters + " AND " + getItemIdsFilter();
        if (workflowId != null)
            filters = filters + " AND " + getWorkflowId();
        if (stateId != null)
            filters = filters + " AND " + getStateId();
        if (classID != null)
            filters = filters + " AND " + getClassID();
        if (parentId != null)
            filters = filters + " AND " + getParentId();
        if (lastChange != null)
            filters = filters + " AND " + getLastChange();
        if (languageShortName != null) {
            filters = filters + " AND " + getLanguageShortName();
            setLanguage(true);
        }
        if (label != null)
            filters = filters + " AND " + getLabel();
        if (isFolder != null)
            filters = filters + " AND " + getIsFolder();
        if (externalKey != null)
            filters = filters + " AND " + getExternalKey();
        if (creationDate != null)
            filters = filters + " AND " + getCreationDate();
        if (copyOf != null)
            filters = filters + " AND " + getCopyOf();
        if (checkoutUser != null)
            filters = filters + " AND " + getCheckoutUser();
        if (authorId != null)
            filters = filters + " AND " + getAuthorId();
        if (author != null)
            filters = filters + " AND " + getAuthor();
        if (parents != null)
            filters = filters + " AND " + getParents();
        if (customAttribute != null && customAttribute != "")
            filters = filters + " AND " + getCustomAttribute();
        if (userDefineFilter != null)
            filters = filters + " AND " + getUserDefineFilter();
        if (languageId != null)
            filters = filters + " AND " + getLanguageId();
        if (filters.equals("")) {
            return null;
        } else {
            return filters.substring(4);
        }
    }
}
