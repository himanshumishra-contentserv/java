package com.exportstaging.testcases;

import com.exportstaging.domain.ConfigurationMessage;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class ConfigurationMessageTest {

    private static ConfigurationMessage confMsg;

    @BeforeClass
    public static void setup() {
        try {
            ConfigurationMessageTest confMsgTest = new ConfigurationMessageTest();
            ClassLoader classLoader = confMsgTest.getClassLoader();
            String jsonMessage = IOUtils.toString(classLoader.getResourceAsStream("pim_conf_185.json"));
            confMsg = new ConfigurationMessage(jsonMessage, "Configuration", "185", 1, 0);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getConfigurationMessageTest() {
        Map<String, String> confData = confMsg.getConfigurationData();
        Assert.assertNotNull(confData);
        Assert.assertEquals(confData.keySet().size(), 46);
        Assert.assertEquals(confData.get("ID"), "185");
        Assert.assertEquals(confData.get("LastChange"), "2017-01-19 11:55:15");
        Assert.assertEquals(confData.get("CreationDate"), "0000-00-00 00:00:00");
        Assert.assertEquals(confData.get("LastEditor"), "Max Mustermann");
        Assert.assertEquals(confData.get("LastEditorID"), "2");
        Assert.assertEquals(confData.get("Author"), "");
        Assert.assertEquals(confData.get("AuthorID"), "0");
        Assert.assertEquals(confData.get("CopyOf"), "0");
        Assert.assertEquals(confData.get("ParentID"), "113");
        Assert.assertEquals(confData.get("Name"), "Name");
        Assert.assertEquals(confData.get("IsFolder"), "0");
        Assert.assertEquals(confData.get("SortOrder"), "-7");
        Assert.assertEquals(confData.get("ExternalKey"), "");
        Assert.assertEquals(confData.get("IsLink"), "0");
        Assert.assertEquals(confData.get("LinkedIDs"), "");
        Assert.assertEquals(confData.get("Type"), "caption");
        Assert.assertEquals(confData.get("Label"), "Name");
        Assert.assertEquals(confData.get("DefaultValue"), "Standard");
        Assert.assertEquals(confData.get("Description"), "");
        Assert.assertEquals(confData.get("LanguageDependent"), "0");
        Assert.assertEquals(confData.get("PaneTitle"), "");
        Assert.assertEquals(confData.get("SectionTitle"), "");
        Assert.assertEquals(confData.get("ParamA"), "");
        Assert.assertEquals(confData.get("ParamB"), "");
        Assert.assertEquals(confData.get("ParamC"), "");
        Assert.assertEquals(confData.get("ParamD"), "");
        Assert.assertEquals(confData.get("ParamE"), "0");
        Assert.assertEquals(confData.get("ParamF"), "");
        Assert.assertEquals(confData.get("ParamG"), "");
        Assert.assertEquals(confData.get("ParamH"), "");
        Assert.assertEquals(confData.get("ParamI"), "");
        Assert.assertEquals(confData.get("ParamJ"), "");
        Assert.assertEquals(confData.get("IsInherited"), "0");
        Assert.assertEquals(confData.get("IsLocale"), "0");
        Assert.assertEquals(confData.get("ShowInList"), "0");
        Assert.assertEquals(confData.get("IsRequired"), "0");
        Assert.assertEquals(confData.get("IsSearchable"), "0");
        Assert.assertEquals(confData.get("SearchIndex"), "0");
        Assert.assertEquals(confData.get("Tags"), "");
        Assert.assertEquals(confData.get("WorkflowID"), "-1");
        Assert.assertEquals(confData.get("ItemrightIDs"), "");
        Assert.assertEquals(confData.get("AllowedClasses"), "");
        Assert.assertEquals(confData.get("PropertyValues"), "{\"IsPreviewImage\":\"0\",\"ReplaceCSFields\":\"0\",\"SpellChecker\":\"0\"}");
        Assert.assertEquals(confData.get("_Module"), "Pdmarticle");
        Assert.assertEquals(confData.get("PaneTitleSortOrder"), "");
    }

    private ClassLoader getClassLoader() {

        return getClass().getClassLoader();
    }
}
