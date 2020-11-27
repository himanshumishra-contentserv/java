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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExternalAPIFileTest {

    private static ItemAPIs itemAPIs;
    private static final String itemType = ItemAPIs.ITEM_TYPE_FILE;

    @BeforeClass
    public static void setUp() throws ExportStagingException, JsonProcessingException {
        //ExportMiscellaneousUtils.configurePropertiesPaths();
        itemAPIs = new ExternalItemAPI(itemType);
    }

    @Test
    public void testGetItemById() throws ExportStagingException {
        RecordWrapper itemList = itemAPIs.getItemById(769);
        itemTestCasesForDefaultLanguage(itemList);
        itemTestCasesForLanguage(itemList, "en");
    }

    @Test
    public void testGetItemByExternalKey() throws ExportStagingException {
        RecordWrapper itemList = itemAPIs.getItemByExternalKey("CS-769");
        //discuss -  replica 0 issue
        itemTestCasesForDefaultLanguage(itemList);
        itemTestCasesForLanguage(itemList, "en");
    }

    @Test
    public void testGetItemIdsByFilters() throws ExportStagingException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        Set<Long> itemIdSet;

        searchFilterCriteria.setLanguageShortName("en");
        searchFilterCriteria.setClassID("121");
        searchFilterCriteria.setAuthorId("4");
        searchFilterCriteria.setLanguageId("1"); //extra to run

        //TODO solve after -  replica 0 issue
        itemIdSet = itemAPIs.getItemIdsByFilters(searchFilterCriteria);
        Assert.assertNotNull(itemIdSet);
        Assert.assertEquals(713, itemIdSet.size());
    }

    @Test
    public void testGetItemsByFilter() throws ExportStagingException, InterruptedException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setLanguageShortName("en");
        searchFilterCriteria.setLanguageId("1"); //extra to run
        ItemsResultSet itemsResultSet = itemAPIs.getItemsByFilter(searchFilterCriteria);
        Assert.assertEquals(1374, itemsResultSet.count());
        Assert.assertNotNull(itemsResultSet);
        List<Long> languageIDs = new ArrayList<>();
        languageIDs.add(1L);
        languageIDs.add(2L);
        for (int index = 0; index < itemsResultSet.count(); index++) {
            RecordWrapper recordWrapper = itemsResultSet.nextItem(languageIDs);
            Assert.assertNotNull(recordWrapper);
            if(recordWrapper.getItemID() == 769){
                itemTestCasesForLanguage(recordWrapper, "en");
            }
        }
    }

    @Test
    public void testGetAttributeByID() throws ExportStagingException, IOException, ParseException {
        Attribute attribute = itemAPIs.getAttributeByID(304);
        configurationTestCases(attribute);
    }

    @Test
    public void testGetAttributeByExternalKey() throws ExportStagingException, ParseException, IOException {
        Attribute attribute = itemAPIs.getAttributeByExternalKey("MAMExportPreset");
        configurationTestCases(attribute);
    }

    @Test
    public void testGetAttributeIDsByClassID() throws ExportStagingException, IOException, ParseException {
        List<Long> attributeList = itemAPIs.getAttributeIDsByClassID(121);
        Assert.assertNotNull(attributeList);
        Assert.assertTrue(attributeList.size() > 0);
        Assert.assertEquals(57, attributeList.size());
    }

    @Test
    public void testGetUpdatedItemIDs() throws ExportStagingException {
        Set<Long> itemIds = itemAPIs.getUpdatedItemIDs("2018-06-12 12:27:02");
        Assert.assertEquals(6, itemIds.size());
    }

    @Test
    public void testGetCreatedItemIDs() throws ExportStagingException{
        Set<Long> itemIds = itemAPIs.getCreatedItemIDs("2018-06-12 12:02:54");
        Assert.assertEquals(1360, itemIds.size());
    }

    @Test
    public void testGetUpdatedItemIdsByFilter() throws ExportStagingException{
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setLanguageId("4");
        searchFilterCriteria.setParentId("3");
        Set<Long> itemIds = itemAPIs.getUpdatedItemIDs("2018-06-12 12:27:02", searchFilterCriteria);
        Assert.assertNotNull(itemIds);
        Assert.assertEquals(2, itemIds.size());

    }

    @Test
    public void testGetCreatedItemIdsByFilter() throws ExportStagingException{
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setLanguageId("3");
        searchFilterCriteria.setCopyOf("7621");
        Set<Long> itemIds = itemAPIs.getCreatedItemIDs("2018-06-12 12:02:54", searchFilterCriteria);
        Assert.assertNotNull(itemIds);
        Assert.assertEquals(2, itemIds.size());
    }

    @Test
    public void testGetLanguageIds() throws ExportStagingException {
        Assert.assertNotNull(itemAPIs.getLanguageIDs());
        Assert.assertEquals(itemAPIs.getLanguageIDs().size(), 6);
        Assert.assertNotNull(itemAPIs.getLanguageShortNames());
        Assert.assertEquals(itemAPIs.getLanguageShortNames().get(2), "de");
    }

    private void configurationTestCases(Attribute attribute) throws IOException, ParseException {
        Assert.assertNotNull(attribute);
        Map<String, String> propValue = attribute.getPropertyValues();
        Assert.assertNotNull(propValue);
        Assert.assertEquals("304", attribute.getId());
        Assert.assertEquals("MAMExportPreset", attribute.getLabel());
        Assert.assertEquals("MAMExportPreset", attribute.getExternalKey());
        Assert.assertEquals("0", attribute.getIsFolder());
        Assert.assertEquals("0", attribute.getIsClass());
        Assert.assertEquals("GUI_PANE_PROPERTIES", attribute.getPaneTitle());
        Assert.assertEquals("imagePresetSelection", attribute.getType());
    }

    protected void itemTestCasesForLanguage(RecordWrapper recordWrapper, String languageShortName) throws ExportStagingException {
        Assert.assertNotNull(recordWrapper);
        ExportValues exportValues = recordWrapper.getValues(languageShortName);
        Assert.assertNotNull(exportValues);
        List<AttributeValue> attributesList = exportValues.getAttributeList();
        List<Reference> referenceList = exportValues.getReferenceList();
        List<Subtable> subtableList = exportValues.getSubtableList();
        Assert.assertNotNull(attributesList);
        Assert.assertNotNull(referenceList);
        Assert.assertNotNull(subtableList);
        Assert.assertEquals(164, attributesList.size());
        Assert.assertEquals(1, referenceList.size());
        Assert.assertEquals(0, subtableList.size());
        Assert.assertEquals("Blue.jpg", recordWrapper.getValue("Label" ,languageShortName));
        Assert.assertEquals(" 8271 8602 8607 769 ", recordWrapper.getValue("_Parents", languageShortName));
        Assert.assertEquals(" 121 ", recordWrapper.getValue("ClassMapping", languageShortName));
        Assert.assertEquals("Blue.jpg", recordWrapper.getValue("FileName", languageShortName));
        Assert.assertNotNull(recordWrapper.getValue("_FilePath", languageShortName));
        Assert.assertNotNull(recordWrapper.getValue("_SystemPath", languageShortName));
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("138")) {
                Assert.assertEquals(1, attributeValue.getLanguageID());
                Assert.assertEquals("2019-12-31", attributeValue.getValue());
                Assert.assertEquals("12/31/2019", attributeValue.getFormattedValue());
            }
            if (attributeValue.getId().equals("-3996")) {
                Assert.assertEquals(1, attributeValue.getLanguageID());
                Assert.assertEquals("Generic", attributeValue.getValue());
                Assert.assertEquals("Generic", attributeValue.getFormattedValue());
            }
        }
        //For Reference
        for (Reference reference : referenceList) {
            if (reference.getReferenceID() == 2543) {
                Assert.assertEquals(769, reference.getItemID());
                Assert.assertEquals(134, reference.getAttributeID());
                Assert.assertEquals("Mamfile", reference.getItemType());
                Assert.assertEquals(1, reference.getLanguageID());
                Assert.assertEquals(2543, reference.getReferenceID());
                Assert.assertEquals(28, reference.getTargetID());
                Assert.assertEquals("User", reference.getTargetType());
                Assert.assertEquals(0, reference.getConfigurationID());
                Assert.assertEquals(0, reference.getAttributeIDs().size());
            }
        }
        //Not a single row for Subtable
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
        Assert.assertEquals(164, attributesList.size());
        Assert.assertEquals(1, referenceList.size());
        Assert.assertEquals(0, subtableList.size());
        Assert.assertEquals("Blue.jpg", recordWrapper.getValue("Label"));
        Assert.assertEquals(" 8271 8602 8607 769 ", recordWrapper.getValue("_Parents"));
        Assert.assertEquals(" 121 ", recordWrapper.getValue("ClassMapping"));
        Assert.assertEquals("Blue.jpg", recordWrapper.getValue("FileName"));
        Assert.assertNotNull(recordWrapper.getValue("_FilePath"));
        Assert.assertNotNull(recordWrapper.getValue("_SystemPath"));
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("138")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("2019-12-31", attributeValue.getValue());
                Assert.assertEquals("12/31/2019", attributeValue.getFormattedValue());
            }
            if (attributeValue.getId().equals("-3996")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("Generic", attributeValue.getValue());
                Assert.assertEquals("Generic", attributeValue.getFormattedValue());
            }
        }
        //For Reference
        for (Reference reference : referenceList) {
            if (reference.getReferenceID() == 2543) {
                Assert.assertEquals(769, reference.getItemID());
                Assert.assertEquals(134, reference.getAttributeID());
                Assert.assertEquals("Mamfile", reference.getItemType());
                Assert.assertEquals(2, reference.getLanguageID());
                Assert.assertEquals(2543, reference.getReferenceID());
                Assert.assertEquals(28, reference.getTargetID());
                Assert.assertEquals("User", reference.getTargetType());
                Assert.assertEquals(0, reference.getConfigurationID());
                Assert.assertEquals(0,reference.getAttributeIDs().size());
            }
        }
        //Not single row for Subtable
    }

    @AfterClass
    public static void tearDown() {
        //itemAPIs.close();
        //itemAPIs = null;
    }

}
