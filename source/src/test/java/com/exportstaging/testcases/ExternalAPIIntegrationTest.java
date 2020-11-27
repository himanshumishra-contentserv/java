package com.exportstaging.testcases;

import com.exportstaging.api.domain.ExportValues;
import com.exportstaging.api.ExternalItemAPI;
import com.exportstaging.api.RecordAPIs;
import com.exportstaging.api.wraper.RecordWrapper;
import com.exportstaging.api.ItemAPIs;
import com.exportstaging.api.domain.AttributeValue;
import com.exportstaging.api.domain.Reference;
import com.exportstaging.api.domain.Subtable;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ExternalAPIIntegrationTest {

    private static ItemAPIs itemAPIsPIM;
    private static ItemAPIs itemAPIsPIMView;
    private static ItemAPIs itemAPIsFile;
    private static ItemAPIs itemAPIsUser;

    private static RecordAPIs recordAPIsWorkflow;

    @BeforeClass
    public static void setUp() throws ExportStagingException, JsonProcessingException {
        //ExportMiscellaneousUtils.configurePropertiesPaths();
        /*itemAPIsPIM = new ExternalItemAPI(ExternalItemAPI.ITEM_TYPE_PRODUCT);
        itemAPIsPIMView = new ExternalItemAPI(ItemAPIs.ITEM_TYPE_VIEW);
        itemAPIsFile = new ExternalItemAPI(ItemAPIs.ITEM_TYPE_FILE);
        itemAPIsUser = new ExternalItemAPI(ItemAPIs.ITEM_TYPE_USER);
        recordAPIsWorkflow = new ExternalItemAPI(RecordAPIs.ITEM_TYPE_WORKFLOW);*/
    }

    @Test
    public void testItemAPIsForPIM() throws ExportStagingException {
        itemAPIsPIM = new ExternalItemAPI(ExternalItemAPI.ITEM_TYPE_PRODUCT);
        RecordWrapper itemList = itemAPIsPIM.getItemById(62);
        Assert.assertNotNull(itemList);
        ExportValues exportValues = itemList.getValues();
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
        //For Simple Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("38")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("99", attributeValue.getValue());
                Assert.assertEquals("99 kg", attributeValue.getFormattedValue());
            }
            if (attributeValue.getId().equalsIgnoreCase("-27201")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("56", attributeValue.getValue());
                Assert.assertEquals("Datasheet PDF", attributeValue.getFormattedValue());
            }
        }
    }

    @Test
    public void testItemAPIsForPIMView() throws ExportStagingException, JsonProcessingException {
        itemAPIsPIMView = new ExternalItemAPI(ItemAPIs.ITEM_TYPE_VIEW);
        RecordWrapper itemList = itemAPIsPIMView.getItemById(246);
        Assert.assertNotNull(itemList);
        ExportValues exportValues = itemList.getValues();
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
    }

    @Test
    public void testItemAPIsForFile() throws ExportStagingException, JsonProcessingException {
        itemAPIsFile = new ExternalItemAPI(ItemAPIs.ITEM_TYPE_FILE);
        RecordWrapper itemList = itemAPIsFile.getItemById(769);
        Assert.assertNotNull(itemList);
        ExportValues exportValues = itemList.getValues();
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
    }

    @Test
    public void testItemAPIsForUser() throws ExportStagingException, JsonProcessingException {
        itemAPIsUser = new ExternalItemAPI(ItemAPIs.ITEM_TYPE_USER);
        RecordWrapper itemList = itemAPIsUser.getItemById(2);
        Assert.assertNotNull(itemList);
        ExportValues exportValues = itemList.getValues();
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

    @Test
    public void testItemAPIsForWorkflow() throws ExportStagingException, JsonProcessingException {
        recordAPIsWorkflow = new ExternalItemAPI(RecordAPIs.ITEM_TYPE_WORKFLOW);
        RecordWrapper recordList = recordAPIsWorkflow.getItemById(9);
        Assert.assertNotNull(recordList);
        ExportValues exportValues = recordList.getValues();
        Assert.assertNotNull(exportValues);
        List<AttributeValue> attributesList = exportValues.getAttributeList();
        Assert.assertNotNull(attributesList);
        Assert.assertEquals(18, attributesList.size());
        Assert.assertEquals("PdmArticle", recordList.getValue("Type"));
        Assert.assertEquals("Products Workflow", recordList.getValue("WorkflowName"));
        Assert.assertEquals(7,recordList.getStateIDs().size());
        //For Standard Attribute
        for (AttributeValue attributeValue : attributesList) {
            if (attributeValue.getId().equals("StateID")) {
                Assert.assertEquals(2, attributeValue.getLanguageID());
                Assert.assertEquals("0", attributeValue.getValue());
                Assert.assertEquals("0", attributeValue.getFormattedValue());
            }
        }
    }


    @AfterClass
    public static void tearDown() {
        /*itemAPIsPIM.close();
        itemAPIsPIMView.close();
        itemAPIsFile.close();*/
        /*itemAPIsPIM = null;
        itemAPIsPIMView = null;
        itemAPIsFile = null;*/
    }

}
