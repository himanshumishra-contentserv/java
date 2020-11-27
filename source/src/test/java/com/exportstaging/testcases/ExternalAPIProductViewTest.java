package com.exportstaging.testcases;

import com.exportstaging.api.ExternalItemAPI;
import com.exportstaging.api.ItemAPIs;
import com.exportstaging.api.domain.*;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.resultset.ItemsResultSet;
import com.exportstaging.api.searchfilter.SearchFilterCriteria;
import com.exportstaging.api.wraper.RecordWrapper;
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

public class ExternalAPIProductViewTest {

    private static ItemAPIs itemAPIs;
    private static final String itemType = ItemAPIs.ITEM_TYPE_VIEW;

    @BeforeClass
    public static void setUp() throws ExportStagingException {
        itemAPIs = new ExternalItemAPI(itemType);
}

    @Test
    public void testGetItemById() throws ExportStagingException {
        RecordWrapper itemList = itemAPIs.getItemById(246);
        itemTestCasesForDefaultLanguage(itemList);
        itemTestCasesForLanguage(itemList, "en");
    }

    @Test
    public void testGetItemByExternalKey() throws ExportStagingException {
        RecordWrapper itemList = itemAPIs.getItemByExternalKey("");
        itemTestCasesForDefaultLanguage(itemList);
        itemTestCasesForLanguage(itemList, "en");
    }

    @Test
    public void testGetItemIdsByFilters() throws ExportStagingException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        Set<Long> itemIdSet;
        searchFilterCriteria.setLanguageShortName("en");
        searchFilterCriteria.setClassID("61");
        searchFilterCriteria.setAuthorId("2");
        //discuss - replica 0 issue
        itemIdSet = itemAPIs.getItemIdsByFilters(searchFilterCriteria);
        Assert.assertNotNull(itemIdSet);
        Assert.assertEquals(8, itemIdSet.size());
    }

    @Test
    public void testGetItemsByFilter() throws ExportStagingException, InterruptedException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setLanguageShortName("en");
        ItemsResultSet itemsResultSet = itemAPIs.getItemsByFilter(searchFilterCriteria);
        //discuss - replica 0 issue
        Assert.assertEquals(162, itemsResultSet.count());
        Assert.assertNotNull(itemsResultSet);
        List<Long> languageIDs = new ArrayList<>();
        languageIDs.add(1L);
        languageIDs.add(2L);
        for (int index = 0; index < itemsResultSet.count(); index++) {
            RecordWrapper recordWrapper = itemsResultSet.nextItem(languageIDs);
            Assert.assertNotNull(recordWrapper);
            if(recordWrapper.getItemID() == 246){
                itemTestCasesForLanguage(recordWrapper, "en");
            }
        }
    }

    @Test
    public void testGetAttributeByID() throws ExportStagingException, IOException, ParseException {
        Attribute attribute = itemAPIs.getAttributeByID(25);
        configurationTestCases(attribute);
    }

    @Test
    public void testGetAttributeByExternalKey() throws ExportStagingException, ParseException, IOException {
        Attribute attribute = itemAPIs.getAttributeByExternalKey("TEASER");
        configurationTestCases(attribute);
    }

    @Test
    public void testGetAttributeIDsByClassID() throws ExportStagingException, IOException, ParseException {
        List<Long> attributeList = itemAPIs.getAttributeIDsByClassID(102);
        Assert.assertNotNull(attributeList);
        Assert.assertTrue(attributeList.size() > 0);
        Assert.assertEquals(71, attributeList.size());
    }

    @Test
    public void testGetUpdatedItemIDs() throws ExportStagingException {
        Set<Long> itemIds = itemAPIs.getUpdatedItemIDs("2018-06-12 12:11:20");
        Assert.assertEquals(37, itemIds.size());
    }

    @Test
    public void testGetCreatedItemIDs() throws ExportStagingException{
        Set<Long> itemIds = itemAPIs.getCreatedItemIDs("2018-06-12 12:11:17");
        Assert.assertEquals(68, itemIds.size());
    }

    @Test
    public void testGetUpdatedItemIdsByFilter() throws ExportStagingException{
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setStateId("15");
        searchFilterCriteria.setLanguageId("3");
        Set<Long> itemIds = itemAPIs.getUpdatedItemIDs("2018-05-23 15:16:19", searchFilterCriteria);
        Assert.assertNotNull(itemIds);
        Assert.assertEquals(159, itemIds.size());

    }

    @Test
    public void testGetCreatedItemIdsByFilter() throws ExportStagingException{
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setStateId("50", "<");
        searchFilterCriteria.setLanguageId("3");
        Set<Long> itemIds = itemAPIs.getCreatedItemIDs("2018-05-23 15:18:44", searchFilterCriteria);
        Assert.assertNotNull(itemIds);
        Assert.assertEquals(159, itemIds.size());
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
        Assert.assertEquals("25", attribute.getId());
        Assert.assertEquals("Teaser", attribute.getLabel());
        Assert.assertEquals("TEASER", attribute.getExternalKey());
        Assert.assertEquals("0", attribute.getIsFolder());
        Assert.assertEquals("0", attribute.getIsClass());
        Assert.assertEquals("Texte", attribute.getPaneTitle());
        Assert.assertEquals("html", attribute.getType());
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
        Assert.assertEquals(119, attributesList.size());
        Assert.assertEquals(23, referenceList.size());
        Assert.assertEquals(0, subtableList.size());
        Assert.assertEquals(" 128 246 ", recordWrapper.getValue("_Parents", languageShortName));
        Assert.assertEquals("  ", recordWrapper.getValue("ClassMapping", languageShortName));
        Assert.assertEquals("102", recordWrapper.getValue("_ExtensionClassMapping", languageShortName));
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("53")) {
                Assert.assertEquals(1, attributeValue.getLanguageID());
                Assert.assertEquals("Ready for shipment within 24 hours", attributeValue.getValue());
                Assert.assertEquals("Ready for shipment within 24 hours", attributeValue.getFormattedValue());
            }
            if (attributeValue.getId().equals("228")) {
                Assert.assertEquals(1, attributeValue.getLanguageID());
                Assert.assertEquals("1", attributeValue.getValue());
                Assert.assertEquals("Endknoten sind Varianten", attributeValue.getFormattedValue());
            }
            if (attributeValue.getId().equals("-27201")) {
                Assert.assertEquals(1, attributeValue.getLanguageID());
                Assert.assertEquals("56", attributeValue.getValue());
                Assert.assertEquals("Datasheet PDF", attributeValue.getFormattedValue());
            }
        }
        //For Reference
        for (Reference reference : referenceList) {
            if (reference.getReferenceID() == 13528) {
                Assert.assertEquals(246, reference.getItemID());
                Assert.assertEquals(183, reference.getAttributeID());
                Assert.assertEquals("Pdmarticlestructure", reference.getItemType());
                Assert.assertEquals(1, reference.getLanguageID());
                Assert.assertEquals(13528, reference.getReferenceID());
                Assert.assertEquals(1456, reference.getTargetID());
                Assert.assertEquals("Mamfile", reference.getTargetType());
                Assert.assertEquals(0, reference.getConfigurationID());
                //not a single classOfReference attribute with data
                Assert.assertEquals(null, reference.getValueById(13));
                Assert.assertEquals(null, reference.getFormattedValueById(13));
            }
        }
        //For Subtable
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
        Assert.assertEquals(119, attributesList.size());
        Assert.assertEquals(23, referenceList.size());
        Assert.assertEquals(0, subtableList.size());
        Assert.assertEquals(" 128 246 ", recordWrapper.getValue("_Parents"));
        Assert.assertEquals("  ", recordWrapper.getValue("ClassMapping"));
        Assert.assertEquals("102", recordWrapper.getValue("_ExtensionClassMapping"));
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("53")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("Ready for shipment within 24 hours", attributeValue.getValue());
                Assert.assertEquals("Ready for shipment within 24 hours", attributeValue.getFormattedValue());
            }
            if (attributeValue.getId().equals("228")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("1", attributeValue.getValue());
                Assert.assertEquals("Endknoten sind Varianten", attributeValue.getFormattedValue());
            }
            if (attributeValue.getId().equals("-27201")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("56", attributeValue.getValue());
                Assert.assertEquals("Datasheet PDF", attributeValue.getFormattedValue());
            }
        }
        //For Reference
        for (Reference reference : referenceList) {
            if (reference.getReferenceID() == 13528) {
                Assert.assertEquals(246, reference.getItemID());
                Assert.assertEquals(183, reference.getAttributeID());
                Assert.assertEquals("Pdmarticlestructure", reference.getItemType());
                Assert.assertEquals(2, reference.getLanguageID());
                Assert.assertEquals(13528, reference.getReferenceID());
                Assert.assertEquals(1456, reference.getTargetID());
                Assert.assertEquals("Mamfile", reference.getTargetType());
                Assert.assertEquals(0, reference.getConfigurationID());
                //not a single classOfReference attribute with data
                Assert.assertEquals(null, reference.getValueById(13));
                Assert.assertEquals(null, reference.getFormattedValueById(13));
            }
        }
        //For Subtable
        // No subtable data with Default backup
    }


    @AfterClass
    public static void tearDown() {
        //externalItemAPI.close();
        //itemAPIs = null;
    }
}
