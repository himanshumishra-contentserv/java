package com.exportstaging.testcases;

import com.exportstaging.api.domain.ExportValues;
import com.exportstaging.api.ExternalItemAPI;
import com.exportstaging.api.resultset.ItemsResultSet;
import com.exportstaging.api.searchfilter.SearchFilterCriteria;
import com.exportstaging.api.domain.AttributeValue;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.RecordAPIs;
import com.exportstaging.api.wraper.RecordWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * Created by CS36 on 21-02-2017.
 */
public class ExternalAPIWorkflowTest {
    private static RecordAPIs recordAPIs;
    private static final String recordType = RecordAPIs.ITEM_TYPE_WORKFLOW;

    @BeforeClass
    public static void setUp() throws ExportStagingException, JsonProcessingException {
        recordAPIs = new ExternalItemAPI(recordType);
    }

    @Test
    public void testGetItemById() throws ExportStagingException {
        RecordWrapper itemList = recordAPIs.getItemById(9);
        recordTestCases(itemList);
    }

    @Test
    public void testGetItemByExternalKey() throws ExportStagingException {
        RecordWrapper itemList = recordAPIs.getItemByExternalKey("PIM");
        recordTestCases(itemList);
    }

    @Test
    public void testGetItemByStateID() throws ExportStagingException {
        RecordWrapper itemList = recordAPIs.getItemByStateID(36);
        itemTestCasesUsingState(itemList);
    }

    @Test
    public void testGetItemIdsByFilters() throws ExportStagingException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        Set<Long> itemIdSet;
        searchFilterCriteria.setStateId("0");

        itemIdSet = recordAPIs.getItemIdsByFilters(searchFilterCriteria);
        Assert.assertNotNull(itemIdSet);
        Assert.assertEquals(20, itemIdSet.size());
    }

    @Test
    public void testGetItemsByFilter() throws ExportStagingException, InterruptedException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setExternalKey("");
        ItemsResultSet itemsResultSet = recordAPIs.getItemsByFilter(searchFilterCriteria);
        Assert.assertEquals(19, itemsResultSet.count());
        Assert.assertNotNull(itemsResultSet);
        for (int index = 0; index < itemsResultSet.count(); index++) {
            RecordWrapper recordWrapper = itemsResultSet.nextItem();
            Assert.assertNotNull(recordWrapper);
            if(recordWrapper.getItemID() == 23){
                itemTestCasesForLanguage(recordWrapper, "de");
            }
        }
    }

    private void recordTestCases(RecordWrapper recordWrapper) throws ExportStagingException {
        Assert.assertNotNull(recordWrapper);
        ExportValues exportValues = recordWrapper.getValues();
        Assert.assertNotNull(exportValues);
        List<AttributeValue> attributesList = exportValues.getAttributeList();
        Assert.assertNotNull(attributesList);
        Assert.assertEquals(18, attributesList.size());
        Assert.assertEquals("PdmArticle", recordWrapper.getValue("Type"));
        Assert.assertEquals("Products Workflow", recordWrapper.getValue("WorkflowName"));
        Assert.assertEquals(7,recordWrapper.getStateIDs().size());
        //For Standard Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("StateID")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("0", attributeValue.getValue());
                Assert.assertEquals("0", attributeValue.getFormattedValue());
            }
        }
    }

    private void itemTestCasesForLanguage(RecordWrapper recordWrapper, String languageShortName) throws ExportStagingException {
        Assert.assertNotNull(recordWrapper);
        ExportValues exportValues = recordWrapper.getValues(languageShortName);
        Assert.assertNotNull(exportValues);
        List<AttributeValue> attributesList = exportValues.getAttributeList();
        Assert.assertNotNull(attributesList);
        Assert.assertEquals(18, attributesList.size());
        Assert.assertEquals("PdmArticle", recordWrapper.getValue("Type"));
        Assert.assertEquals("Default Workflow", recordWrapper.getValue("WorkflowName"));
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("StateID")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("0", attributeValue.getValue());
                Assert.assertEquals("0", attributeValue.getFormattedValue());
            }
        }
    }

    private void itemTestCasesUsingState(RecordWrapper recordWrapper) throws ExportStagingException {
        Assert.assertNotNull(recordWrapper);
        ExportValues exportValues = recordWrapper.getValues();
        Assert.assertNotNull(exportValues);
        List<AttributeValue> attributesList = exportValues.getAttributeList();
        Assert.assertNotNull(attributesList);
        Assert.assertEquals(18, attributesList.size());
        Assert.assertEquals("Rectangle", recordWrapper.getValue("Type"));
        Assert.assertEquals("Created", recordWrapper.getValue("StateName"));
        //For Standard Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("ColorID")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("1", attributeValue.getValue());
                Assert.assertEquals("1", attributeValue.getFormattedValue());
            }
        }
    }
}
