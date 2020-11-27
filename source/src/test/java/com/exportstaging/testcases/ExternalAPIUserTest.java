package com.exportstaging.testcases;

import com.exportstaging.api.ExternalItemAPI;
import com.exportstaging.api.ItemAPIs;
import com.exportstaging.api.domain.*;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.resultset.ItemsResultSet;
import com.exportstaging.api.searchfilter.SearchFilterCriteria;
import com.exportstaging.api.wraper.RecordWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExternalAPIUserTest {
    private static ItemAPIs itemAPIs;
    private static final String itemType = ItemAPIs.ITEM_TYPE_USER;

    @BeforeClass
    public static void setUp() throws ExportStagingException, JsonProcessingException {
        itemAPIs = new ExternalItemAPI(itemType);
    }

    @Test
    public void testGetItemById() throws ExportStagingException {
        RecordWrapper itemList = itemAPIs.getItemById(2);
        itemTestCasesForDefaultLanguage(itemList);
        itemTestCasesForLanguage(itemList, "en");
    }

    @Test
    public void testGetItemByExternalKey() throws ExportStagingException {
        RecordWrapper itemList = itemAPIs.getItemByExternalKey("AWO");
        itemTestCasesForDefaultLanguage(itemList);
        itemTestCasesForLanguage(itemList, "en");
    }

    @Test
    public void testGetItemIdsByFilters() throws ExportStagingException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        Set<Long> itemIdSet;
        searchFilterCriteria.setLanguageShortName("en");
        searchFilterCriteria.setClassID("10");
        searchFilterCriteria.setParents("217");

        itemIdSet = itemAPIs.getItemIdsByFilters(searchFilterCriteria);
        Assert.assertNotNull(itemIdSet);
        Assert.assertEquals(5, itemIdSet.size());  //discuss - replica error
    }

    @Test
    public void testGetItemsByFilter() throws ExportStagingException, InterruptedException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setLanguageShortName("en");
        ItemsResultSet itemsResultSet = itemAPIs.getItemsByFilter(searchFilterCriteria);
        Assert.assertNotNull(itemsResultSet);
        Assert.assertEquals(211, itemsResultSet.count());
        List<Long> languageIDs = new ArrayList<>();
        languageIDs.add(1L);
        languageIDs.add(2L);
        for (int index = 0; index < itemsResultSet.count(); index++) {
            RecordWrapper recordWrapper = itemsResultSet.nextItem(languageIDs);
            Assert.assertNotNull(recordWrapper);
            if (recordWrapper.getItemID() == 2) {
                itemTestCasesForLanguage(recordWrapper, "en");
            }
        }
    }

    @Test
    public void testGetAttributeByID() throws ExportStagingException, IOException, ParseException {
        Attribute attribute = itemAPIs.getAttributeByID(21);
        configurationTestCases(attribute);
    }

    @Test
    public void testGetAttributeByExternalKey() throws ExportStagingException, ParseException, IOException {
        Attribute attribute = itemAPIs.getAttributeByExternalKey("");
        Assert.assertNull(attribute);
    }

    @Test
    public void testGetAttributeIDsByClassID() throws ExportStagingException, IOException, ParseException {
        List<Long> attributeList = itemAPIs.getAttributeIDsByClassID(18);
        Assert.assertNotNull(attributeList);
        Assert.assertTrue(attributeList.size() > 0);
        Assert.assertEquals(11, attributeList.size());
        Attribute attribute = itemAPIs.getAttributeByID(attributeList.get(1));
        configurationTestCases(attribute);
    }

    @Test
    public void testGetUpdatedItemIDs() throws ExportStagingException {
        Set<Long> itemIds = itemAPIs.getUpdatedItemIDs("2018-06-12 12:03:54");
        Assert.assertEquals(198, itemIds.size());
    }

    @Test
    public void testGetCreatedItemIDs() throws ExportStagingException {
        Set<Long> itemIds = itemAPIs.getCreatedItemIDs("2018-06-12 12:03:57");
        Assert.assertEquals(190, itemIds.size());
    }

    @Test
    public void testGetUpdatedItemIdsByFilter() throws ExportStagingException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setLanguageId("4");
        searchFilterCriteria.setParentId("234");
        Set<Long> itemIds = itemAPIs.getUpdatedItemIDs("2018-06-12 12:03:54", searchFilterCriteria);
        Assert.assertNotNull(itemIds);
        Assert.assertEquals(182, itemIds.size());

    }

    @Test
    public void testGetCreatedItemIdsByFilter() throws ExportStagingException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setLanguageId("3");
        searchFilterCriteria.setClassID("10");
        Set<Long> itemIds = itemAPIs.getCreatedItemIDs("2018-06-12 12:03:57", searchFilterCriteria);
        Assert.assertNotNull(itemIds);
        Assert.assertEquals(1, itemIds.size());
    }

    @Test
    public void testGetLanguageIds() throws ExportStagingException {
        Assert.assertNotNull(itemAPIs.getLanguageIDs());
        Assert.assertEquals(itemAPIs.getLanguageIDs().size(), 6);
        Assert.assertNotNull(itemAPIs.getLanguageShortNames());
        Assert.assertEquals(itemAPIs.getLanguageShortNames().get(2), "de");
    }

    private void itemTestCasesForDefaultLanguage(RecordWrapper recordWrapper) throws ExportStagingException {
        Assert.assertNotNull(recordWrapper);
        ExportValues exportValues = recordWrapper.getValues();
        Assert.assertNotNull(exportValues);
        List<AttributeValue> attributesList = exportValues.getAttributeList();
        List<Reference> referenceList = exportValues.getReferenceList();
        List<Subtable> subtableList = exportValues.getSubtableList();
        Assert.assertNotNull(attributesList);
        Assert.assertNotNull(referenceList);
        Assert.assertNotNull(subtableList);
        Assert.assertEquals(95, attributesList.size());
        Assert.assertEquals(0, referenceList.size());
        Assert.assertEquals(0, subtableList.size());
        Assert.assertEquals(" 229 2 ", recordWrapper.getValue("_Parents"));
        Assert.assertEquals("  ", recordWrapper.getValue("ClassMapping"));
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            // not a single simple attribute with data for User-2
            if (attributeValue.getId().equals("22")) {
                //this block is never executed // discuss
            }
            if (attributeValue.getId().equals("-18402")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("3097\n3099", attributeValue.getValue());
                Assert.assertEquals("Editors\nGraphics", attributeValue.getFormattedValue());
            }
        }
    }

    private void itemTestCasesForLanguage(RecordWrapper recordWrapper, String languageShortName) throws ExportStagingException {
        Assert.assertNotNull(recordWrapper);
        ExportValues exportValues = recordWrapper.getValues(languageShortName);
        Assert.assertNotNull(exportValues);
        List<AttributeValue> attributesList = exportValues.getAttributeList();
        List<Reference> referenceList = exportValues.getReferenceList();
        List<Subtable> subtableList = exportValues.getSubtableList();
        Assert.assertNotNull(attributesList);
        Assert.assertNotNull(referenceList);
        Assert.assertNotNull(subtableList);
        Assert.assertEquals(95, attributesList.size());
        Assert.assertEquals(0, referenceList.size());
        Assert.assertEquals(0, subtableList.size());
        Assert.assertEquals(" 229 2 ", recordWrapper.getValue("_Parents"));
        Assert.assertEquals("  ", recordWrapper.getValue("ClassMapping"));
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            // not a single simple attribute with data for User-2
            if (attributeValue.getId().equals("22")) {
                //this block is never executed // discuss
            }
            if (attributeValue.getId().equals("-18402")) {
                Assert.assertEquals(1, attributeValue.getLanguageID());
                Assert.assertEquals("3097\n3099", attributeValue.getValue());
                Assert.assertEquals("Editors\nGraphics", attributeValue.getFormattedValue());
            }
        }
    }

    private void configurationTestCases(Attribute attribute) throws IOException, ParseException {
        Assert.assertNotNull(attribute);
        Map<String, String> propValue = attribute.getPropertyValues();
        Assert.assertNotNull(propValue);
        Assert.assertEquals("21", attribute.getId());
        Assert.assertEquals("Profession", attribute.getLabel());
        Assert.assertEquals("", attribute.getExternalKey());
        Assert.assertEquals("0", attribute.getIsFolder());
        Assert.assertEquals("0", attribute.getIsClass());
        Assert.assertEquals("Tags", attribute.getPaneTitle());
        Assert.assertEquals("valuerange", attribute.getType());
        //Assert.assertEquals("34", attribute.getValue("Name")); //discuss
        Assert.assertEquals("34", attribute.getParamA());

    }
}

