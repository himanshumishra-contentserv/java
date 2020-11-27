package com.exportstaging.testcases;

import com.exportstaging.api.ExternalItemAPI;
import com.exportstaging.api.RecordAPIs;
import com.exportstaging.api.exception.ExportStagingException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class ExternalAPILanguageTest {
    private static RecordAPIs recordAPIs;
    private static final String recordType = RecordAPIs.ITEM_TYPE_LANGUAGE;

    @BeforeClass
    public static void setUp() throws ExportStagingException {
        recordAPIs = new ExternalItemAPI(recordType);
    }

    @Test
    public void testGetLanguageById() throws ExportStagingException {
        Map<String, String> language = recordAPIs.getLanguageByID(2);
        Assert.assertNotNull(language);
        Assert.assertEquals(language.get("ID"), "2");
        Assert.assertEquals(language.get("ShortName"), "de");
        Assert.assertEquals(language.get("ParentID"), "1");
    }

    @Test
    public void testGetLanguageByShortName() throws ExportStagingException {
        Map<String, String> language = recordAPIs.getLanguageByShortName("de");
        Assert.assertNotNull(language);
        Assert.assertEquals(language.get("ID"), "2");
        Assert.assertEquals(language.get("ShortName"), "de");
        Assert.assertEquals(language.get("ParentID"), "1");
    }

    @AfterClass
    public static void tearDown() {
//        recordAPIs.close();
    }
}
