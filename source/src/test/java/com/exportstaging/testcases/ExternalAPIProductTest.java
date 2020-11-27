package com.exportstaging.testcases;

import com.exportstaging.api.ExternalItemAPI;
import com.exportstaging.api.ItemAPIs;
import com.exportstaging.api.domain.*;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.resultset.ItemsResultSet;
import com.exportstaging.api.searchfilter.SearchFilterCriteria;
import com.exportstaging.api.wraper.ItemWrapper;
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

public class ExternalAPIProductTest {
    private static ItemAPIs itemAPIs;
    private static final String itemType = ItemAPIs.ITEM_TYPE_PRODUCT;

    @BeforeClass
    public static void setUp() throws ExportStagingException {
        //ExportMiscellaneousUtils.configurePropertiesPaths();
        itemAPIs = new ExternalItemAPI(itemType);
    }

    @Test
    public void testGetItemById() throws ExportStagingException {
        ItemWrapper itemList = itemAPIs.getItemById(62);
        itemTestCasesForDefaultLanguage(itemList);
        itemTestCasesForLanguage(itemList, "en");
    }

    @Test
    public void testGetItemByExternalKey() throws ExportStagingException {
        ItemWrapper itemList = itemAPIs.getItemByExternalKey("CS-62");
        //discuss - replica 0 issue
        itemTestCasesForDefaultLanguage(itemList);
        itemTestCasesForLanguage(itemList, "en");
    }

    @Test
    public void testGetItemIdsByFilters() throws ExportStagingException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        Set<Long> itemIdSet;
        searchFilterCriteria.setLanguageId("1");
        searchFilterCriteria.setLanguageShortName("en");
        searchFilterCriteria.setClassID("102");

        //discuss - replica 0 issue
        itemIdSet = itemAPIs.getItemIdsByFilters(searchFilterCriteria);
        Assert.assertNotNull(itemIdSet);
        Assert.assertEquals(25, itemIdSet.size());
    }

    @Test
    public void testGetItemsByFilter() throws ExportStagingException, InterruptedException {
        //discuss - replica 0 issue
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setLanguageId("1");
        searchFilterCriteria.setLanguageShortName("en");
        ItemsResultSet itemsResultSet = itemAPIs.getItemsByFilter(searchFilterCriteria);
        Assert.assertEquals(84, itemsResultSet.count());
        Assert.assertNotNull(itemsResultSet);
        List<Long> languageIDs = new ArrayList<>();
        languageIDs.add(1L);
        languageIDs.add(2L);
        for (int index = 0; index < itemsResultSet.count(); index++) {
            RecordWrapper recordWrapper = itemsResultSet.nextItem(languageIDs);
            Assert.assertNotNull(recordWrapper);
            if (recordWrapper.getItemID() == 62) {
                itemTestCasesForLanguage(recordWrapper, "en");
            }
        }
    }

    @Test
    public void testGetAttributeByID() throws ExportStagingException, IOException, ParseException {
        Attribute attribute = itemAPIs.getAttributeByID(35);
        configurationTestCases(attribute);
    }

    @Test
    public void testGetAttributeByExternalKey() throws ExportStagingException, ParseException, IOException {
        Attribute attribute = itemAPIs.getAttributeByExternalKey("HEIGHT");
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
        Set<Long> itemIds = itemAPIs.getUpdatedItemIDs("2018-06-12 12:07:42");
        Assert.assertEquals(30, itemIds.size());
    }

    @Test
    public void testGetCreatedItemIDs() throws ExportStagingException {
        Set<Long> itemIds = itemAPIs.getCreatedItemIDs("2018-06-12 12:07:32");
        Assert.assertEquals(47, itemIds.size());
    }

    @Test
    public void testGetUpdatedItemIdsByFilter() throws ExportStagingException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setExternalKey("CS-62");
        searchFilterCriteria.setStateId("38");
        searchFilterCriteria.setLanguageId("3");
        searchFilterCriteria.setLabel("メディア プレーヤー");
        Set<Long> itemIds = itemAPIs.getUpdatedItemIDs("2018-06-12 12:07:43", searchFilterCriteria);
        Assert.assertNotNull(itemIds);
        Assert.assertEquals(1, itemIds.size());

    }

    @Test
    public void testGetCreatedItemIdsByFilter() throws ExportStagingException {
        SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
        searchFilterCriteria.setStateId("38");
        searchFilterCriteria.setLanguageId("3");
        Set<Long> itemIds = itemAPIs.getCreatedItemIDs("2018-06-12 12:07:32", searchFilterCriteria);
        Assert.assertNotNull(itemIds);
        Assert.assertEquals(33, itemIds.size());
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
        Assert.assertEquals("35", attribute.getId());
        Assert.assertEquals("Höhe", attribute.getLabel());
        Assert.assertEquals("HEIGHT", attribute.getExternalKey());
        Assert.assertEquals("0", attribute.getIsFolder());
        Assert.assertEquals("0", attribute.getIsClass());
        Assert.assertEquals("Spezifikationen", attribute.getPaneTitle());
        Assert.assertEquals("measure", attribute.getType());
        Assert.assertEquals("2", attribute.getValue("LastEditorID"));
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
        Assert.assertEquals(109, attributesList.size());
        Assert.assertEquals(22, referenceList.size());
        Assert.assertEquals(8, subtableList.size());
        Assert.assertEquals(" 61 62 ", recordWrapper.getValue("_Parents", languageShortName));
        Assert.assertEquals(" 102 ", recordWrapper.getValue("ClassMapping", languageShortName));
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("38")) {
                Assert.assertEquals(1, attributeValue.getLanguageID());
                Assert.assertEquals("99", attributeValue.getValue());
                Assert.assertEquals("99 kg", attributeValue.getFormattedValue());
            }
            if (attributeValue.getId().equals("-27201")) {
                Assert.assertEquals(1, attributeValue.getLanguageID());
                Assert.assertEquals("56", attributeValue.getValue());
                Assert.assertEquals("Datasheet PDF", attributeValue.getFormattedValue());
            }
        }
        //For Reference
        for (Reference reference : referenceList) {
            if (reference.getReferenceID() == 1395) {
                Assert.assertEquals(62, reference.getItemID());
                Assert.assertEquals(30, reference.getAttributeID());
                Assert.assertEquals("Pdmarticle", reference.getItemType());
                Assert.assertEquals(1, reference.getLanguageID());
                Assert.assertEquals(1395, reference.getReferenceID());
                Assert.assertEquals(85, reference.getTargetID());
                Assert.assertEquals("Pdmarticle", reference.getTargetType());
                Assert.assertEquals(146, reference.getConfigurationID());
                Assert.assertEquals("Weiß", reference.getValueById(13));
                Assert.assertEquals("Weiß", reference.getFormattedValueById(13));
            }
        }
        //For Subtable
        for (Subtable subtable : subtableList) {
            if (subtable.getCSItemTableID() == 20) {
                // Simple attributes
                Assert.assertEquals(62, subtable.getItemID());
                Assert.assertEquals(191, subtable.getAttributeID());
                Assert.assertEquals(1, subtable.getLanguageID());
                Assert.assertEquals("Pdmarticle", subtable.getItemType());
                Assert.assertEquals("3589", subtable.getValueById(187));
                Assert.assertEquals("Euro", subtable.getFormattedValueById(187));
            }
        }
    }

    private void itemTestCasesForDefaultLanguage(ItemWrapper itemWrapper) throws ExportStagingException {
        Assert.assertNotNull(itemWrapper);
        ExportValues exportValues = itemWrapper.getValues();
        Assert.assertNotNull(exportValues);
        List<AttributeValue> attributesList = exportValues.getAttributeList();
        List<Reference> referenceList = exportValues.getReferenceList();
        List<Subtable> subtableList = exportValues.getSubtableList();
        Assert.assertNotNull(attributesList);
        Assert.assertNotNull(referenceList);
        Assert.assertNotNull(subtableList);
        Assert.assertEquals(109, attributesList.size());
        Assert.assertEquals(22, referenceList.size());
        Assert.assertEquals(8, subtableList.size());
        Assert.assertEquals(" 61 62 ", itemWrapper.getValue("_Parents"));
        Assert.assertEquals(" 102 ", itemWrapper.getValue("ClassMapping"));
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("38")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("99", attributeValue.getValue());
                Assert.assertEquals("99 kg", attributeValue.getFormattedValue());
            }
            if (attributeValue.getId().equals("-27201")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("56", attributeValue.getValue());
                Assert.assertEquals("Datasheet PDF", attributeValue.getFormattedValue());
            }
        }
        //For Reference
        for (Reference reference : referenceList) {
            if (reference.getReferenceID() == 1395) {
                Assert.assertEquals(62, reference.getItemID());
                Assert.assertEquals(30, reference.getAttributeID());
                Assert.assertEquals("Pdmarticle", reference.getItemType());
                Assert.assertEquals(2, reference.getLanguageID());
                Assert.assertEquals(1395, reference.getReferenceID());
                Assert.assertEquals(85, reference.getTargetID());
                Assert.assertEquals("Pdmarticle", reference.getTargetType());
                Assert.assertEquals(146, reference.getConfigurationID());
                Assert.assertEquals("Weiß", reference.getValueById(13));
                Assert.assertEquals("Weiß", reference.getFormattedValueById(13));
            }
        }
        //For Subtable
        for (Subtable subtable : subtableList) {
            if (subtable.getCSItemTableID() == 20) {
                // Simple attributes
                Assert.assertEquals(62, subtable.getItemID());
                Assert.assertEquals(191, subtable.getAttributeID());
                Assert.assertEquals(2, subtable.getLanguageID());
                Assert.assertEquals("Pdmarticle", subtable.getItemType());
                Assert.assertEquals("3589", subtable.getValueById(187));
                Assert.assertEquals("Euro", subtable.getFormattedValueById(187));
            }
        }
    }

    @AfterClass
    public static void tearDown() {
        //itemAPIs.close();
        //itemAPIs = null;
    }
}
