package com.exportstaging.testcases;

import com.exportstaging.domain.ItemMessage;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ItemMessageTest {

    private static ItemMessage itemMessage;

    @BeforeClass
    public static void setup() {
        try {
            ItemMessageTest test = new ItemMessageTest();
            ClassLoader classLoader = test.getClassLoader();
            String jsonMessage = IOUtils.toString(classLoader.getResourceAsStream("pim_id_62.json"));
            itemMessage = new ItemMessage(jsonMessage, "62", 1, "Item", 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getItemMessageTest() {

        Map<String, Map<String, String>> itemData = itemMessage.getItemData();
        Assert.assertNotNull(itemData);
        Assert.assertEquals(itemData.keySet().size(), 6);
        Assert.assertNotNull(itemData.get("1"));
        Assert.assertEquals(itemData.get("1").keySet().size(), 167);
        Assert.assertEquals(itemData.get("1").get("ID"), (long)62);
        Assert.assertEquals(itemData.get("1").get("Label"), "Media Player");
        Assert.assertEquals(itemData.get("1").get("ClassMapping"), " 102 421 420 433 ");
        Assert.assertEquals(itemData.get("1").get("CreationDate"), "2007-10-08 22:58:08");
        Assert.assertEquals(itemData.get("1").get("StateID"), "120");
        Assert.assertEquals(itemData.get("1").get("Parents"), "61 62");
        Assert.assertEquals(itemData.get("1").get("AuthorID"), "2");
        Assert.assertEquals(itemData.get("1").get("WorkflowID"), "23");
        Assert.assertEquals(itemData.get("1").get("LastEditorID"), "2");
        Assert.assertEquals(itemData.get("1").get("ExternalKey"), "CS-62");
        Assert.assertEquals(itemData.get("1").get("LanguageID"), "1");
        Assert.assertEquals(itemData.get("1").get("13:FormattedValue"), "Blue");
        Assert.assertEquals(itemData.get("1").get("43:Value"), "5 to 95 %, not condensing");
        Assert.assertEquals(itemData.get("1").get("43:FormattedValue"), "5 to 95 %, not condensing");
        Assert.assertNull(itemData.get("1").get("CopyOf"));
        Assert.assertNull(itemData.get("1").get("26:Value"));
        Assert.assertNotNull(itemData.get("1").get("14:Value"));

        Assert.assertNotNull(itemData.get("2"));
        Assert.assertEquals(itemData.get("2").keySet().size(), 167);
        Assert.assertEquals(itemData.get("2").get("ID"), (long)62);
        Assert.assertEquals(itemData.get("2").get("Label"), "Media Player");
        Assert.assertEquals(itemData.get("2").get("ClassMapping"), " 102 421 420 433 ");
        Assert.assertEquals(itemData.get("2").get("CreationDate"), "2007-10-08 22:58:08");
        Assert.assertEquals(itemData.get("2").get("StateID"), "120");
        Assert.assertEquals(itemData.get("2").get("Parents"), "61 62");
        Assert.assertEquals(itemData.get("2").get("AuthorID"), "2");
        Assert.assertEquals(itemData.get("2").get("WorkflowID"), "23");
        Assert.assertEquals(itemData.get("2").get("LastEditorID"), "2");
        Assert.assertEquals(itemData.get("2").get("ExternalKey"), "CS-62");
        Assert.assertEquals(itemData.get("2").get("LanguageID"), "2");
        Assert.assertEquals(itemData.get("2").get("13:FormattedValue"), "None");
        Assert.assertEquals(itemData.get("2").get("43:Value"), "5 bis 95 %, nicht kondensierend");
        Assert.assertEquals(itemData.get("2").get("43:FormattedValue"), "5 bis 95 %, nicht kondensierend");
        Assert.assertNull(itemData.get("2").get("CopyOf"));
        Assert.assertNull(itemData.get("2").get("26:Value"));
        Assert.assertNotNull(itemData.get("2").get("14:Value"));

        Assert.assertNotNull(itemData.get("3"));
        Assert.assertEquals(itemData.get("3").keySet().size(), 167);
        Assert.assertEquals(itemData.get("3").get("ID"), (long)62);
        Assert.assertEquals(itemData.get("3").get("Label"), "メディア プレーヤー");
        Assert.assertEquals(itemData.get("3").get("ClassMapping"), " 102 421 420 433 ");
        Assert.assertEquals(itemData.get("3").get("CreationDate"), "2007-10-08 22:58:08");
        Assert.assertEquals(itemData.get("3").get("StateID"), "38");
        Assert.assertEquals(itemData.get("3").get("Parents"), "61 62");
        Assert.assertEquals(itemData.get("3").get("AuthorID"), "2");
        Assert.assertEquals(itemData.get("3").get("WorkflowID"), "9");
        Assert.assertEquals(itemData.get("3").get("LastEditorID"), "2");
        Assert.assertEquals(itemData.get("3").get("ExternalKey"), "CS-62");
        Assert.assertEquals(itemData.get("3").get("LanguageID"), "3");
        Assert.assertEquals(itemData.get("3").get("13:FormattedValue"), "青");
        Assert.assertEquals(itemData.get("3").get("43:Value"), "5 ～ 95%、ない結露しないこと");
        Assert.assertEquals(itemData.get("3").get("43:FormattedValue"), "5 ～ 95%、ない結露しないこと");
        Assert.assertNull(itemData.get("3").get("CopyOf"));
        Assert.assertNull(itemData.get("3").get("26:Value"));
        Assert.assertNotNull(itemData.get("3").get("14:Value"));

        Assert.assertNotNull(itemData.get("4"));
        Assert.assertEquals(itemData.get("4").keySet().size(), 167);
        Assert.assertEquals(itemData.get("4").get("ID"), (long)62);
        Assert.assertEquals(itemData.get("4").get("Label"), "Media Player");
        Assert.assertEquals(itemData.get("4").get("ClassMapping"), " 102 421 420 433 ");
        Assert.assertEquals(itemData.get("4").get("CreationDate"), "2007-10-08 22:58:08");
        Assert.assertEquals(itemData.get("4").get("StateID"), "120");
        Assert.assertEquals(itemData.get("4").get("Parents"), "61 62");
        Assert.assertEquals(itemData.get("4").get("AuthorID"), "2");
        Assert.assertEquals(itemData.get("4").get("WorkflowID"), "23");
        Assert.assertEquals(itemData.get("4").get("LastEditorID"), "2");
        Assert.assertEquals(itemData.get("4").get("ExternalKey"), "CS-62");
        Assert.assertEquals(itemData.get("4").get("LanguageID"), "4");
        Assert.assertEquals(itemData.get("4").get("13:FormattedValue"), "Bleu");
        Assert.assertEquals(itemData.get("4").get("43:Value"), "5 à 95 %, ne pas de condensation");
        Assert.assertEquals(itemData.get("4").get("43:FormattedValue"), "5 à 95 %, ne pas de condensation");
        Assert.assertNull(itemData.get("4").get("CopyOf"));
        Assert.assertNull(itemData.get("4").get("26:Value"));
        Assert.assertNotNull(itemData.get("4").get("14:Value"));

        Assert.assertNotNull(itemData.get("5"));
        Assert.assertEquals(itemData.get("5").keySet().size(), 167);
        Assert.assertEquals(itemData.get("5").get("ID"), (long)62);
        Assert.assertEquals(itemData.get("5").get("Label"), "Lettore multimediale");
        Assert.assertEquals(itemData.get("5").get("ClassMapping"), " 102 421 420 433 ");
        Assert.assertEquals(itemData.get("5").get("CreationDate"), "2007-10-08 22:58:08");
        Assert.assertEquals(itemData.get("5").get("StateID"), "36");
        Assert.assertEquals(itemData.get("5").get("Parents"), "61 62");
        Assert.assertEquals(itemData.get("5").get("AuthorID"), "2");
        Assert.assertEquals(itemData.get("5").get("WorkflowID"), "9");
        Assert.assertEquals(itemData.get("5").get("LastEditorID"), "2");
        Assert.assertEquals(itemData.get("5").get("ExternalKey"), "CS-62");
        Assert.assertEquals(itemData.get("5").get("LanguageID"), "5");
        Assert.assertEquals(itemData.get("5").get("13:FormattedValue"), "Blu");
        Assert.assertEquals(itemData.get("5").get("43:Value"), "5-95%, non condensante");
        Assert.assertEquals(itemData.get("5").get("43:FormattedValue"), "5-95%, non condensante");
        Assert.assertNull(itemData.get("5").get("CopyOf"));
        Assert.assertNull(itemData.get("5").get("26:Value"));
        Assert.assertNotNull(itemData.get("5").get("14:Value"));

        Assert.assertNotNull(itemData.get("6"));
        Assert.assertEquals(itemData.get("6").keySet().size(), 167);
        Assert.assertEquals(itemData.get("6").get("ID"), (long)62);
        Assert.assertEquals(itemData.get("6").get("Label"), "Reproductor multimedia");
        Assert.assertEquals(itemData.get("6").get("ClassMapping"), " 102 421 420 433 ");
        Assert.assertEquals(itemData.get("6").get("CreationDate"), "2007-10-08 22:58:08");
        Assert.assertEquals(itemData.get("6").get("StateID"), "36");
        Assert.assertEquals(itemData.get("6").get("Parents"), "61 62");
        Assert.assertEquals(itemData.get("6").get("AuthorID"), "2");
        Assert.assertEquals(itemData.get("6").get("WorkflowID"), "9");
        Assert.assertEquals(itemData.get("6").get("LastEditorID"), "2");
        Assert.assertEquals(itemData.get("6").get("ExternalKey"), "CS-62");
        Assert.assertEquals(itemData.get("6").get("LanguageID"), "6");
        Assert.assertEquals(itemData.get("6").get("13:FormattedValue"), "Azul");
        Assert.assertEquals(itemData.get("6").get("43:Value"), "5 a 95%, no condensada");
        Assert.assertEquals(itemData.get("6").get("43:FormattedValue"), "5 a 95%, no condensada");
        Assert.assertNull(itemData.get("6").get("CopyOf"));
        Assert.assertNull(itemData.get("6").get("26:Value"));
        Assert.assertNotNull(itemData.get("6").get("14:Value"));
    }

    @Test
    public void getReferenceMesageTest() {
        Map<Integer, Map<Integer, List<Map>>> referenceData = itemMessage.getReferenceData();
        Assert.assertNotNull(referenceData);
        Assert.assertEquals(referenceData.size(), 6);
        Assert.assertNotNull(referenceData.get(1));
        Assert.assertEquals(referenceData.get(1).size(), 7);

        List<Map> Attr130 = referenceData.get(1).get(130);
        Assert.assertEquals(Attr130.get(0).get("LanguageID"), "1");
        Assert.assertEquals(Attr130.get(0).get("AttributeID"), "130");
        Assert.assertEquals(Attr130.get(0).get("CSReferenceID"), "1120");
        Assert.assertEquals(Attr130.get(0).get("SourceType"), "Pdmarticle");
        Assert.assertEquals(Attr130.get(0).get("TargetType"), "Pdmarticle");
        Assert.assertEquals(Attr130.get(0).get("SortOrder"), "0");
        Assert.assertNull(Attr130.get(0).get("ClassID"));
        Assert.assertEquals(Attr130.get(0).get("ItemID"), "62");
        Assert.assertEquals(Attr130.get(0).get("SubItemID"), "62");
        Assert.assertEquals(Attr130.get(0).get("TargetID"), "113");

        List<Map> Attr181 = referenceData.get(1).get(181);
        Assert.assertEquals(Attr181.get(1).get("LanguageID"), "1");
        Assert.assertEquals(Attr181.get(1).get("AttributeID"), "181");
        Assert.assertEquals(Attr181.get(1).get("CSReferenceID"), "13526");
        Assert.assertEquals(Attr181.get(1).get("SourceType"), "Pdmarticle");
        Assert.assertEquals(Attr181.get(1).get("TargetType"), "Mamfile");
        Assert.assertEquals(Attr181.get(1).get("SortOrder"), "1");
        Assert.assertNull(Attr181.get(1).get("ClassID"));
        Assert.assertEquals(Attr181.get(1).get("ItemID"), "62");
        Assert.assertEquals(Attr181.get(1).get("SubItemID"), "62");
        Assert.assertEquals(Attr181.get(1).get("TargetID"), "632");

        List<Map> Attr30 = referenceData.get(5).get(30);
        Assert.assertEquals(Attr30.get(1).get("LanguageID"), "5");
        Assert.assertEquals(Attr30.get(1).get("AttributeID"), "30");
        Assert.assertEquals(Attr30.get(1).get("CSReferenceID"), "1396");
        Assert.assertEquals(Attr30.get(1).get("SourceType"), "Pdmarticle");
        Assert.assertEquals(Attr30.get(1).get("TargetType"), "Pdmarticle");
        Assert.assertEquals(Attr30.get(1).get("SortOrder"), "1");
        Assert.assertEquals(Attr30.get(1).get("ClassID"), "146");
        Assert.assertEquals(Attr30.get(1).get("ItemID"), "62");
        Assert.assertEquals(Attr30.get(1).get("SubItemID"), "62");
        Assert.assertEquals(Attr30.get(1).get("TargetID"), "86");
        Assert.assertEquals(Attr30.get(1).get("13:Value"), "clematis");
        Assert.assertEquals(Attr30.get(1).get("13:FormattedValue"), "clematis");
        Assert.assertEquals(Attr30.get(1).get("142:Value"), "KS-930-393-360");
        Assert.assertEquals(Attr30.get(1).get("142:FormattedValue"), "KS-930-393-360");
        Assert.assertEquals(Attr30.get(1).get("147:Value"), "39,80");
        Assert.assertEquals(Attr30.get(1).get("147:FormattedValue"), "39,80");

    }


    @Test
    public void getSubtableMessageTest() {
        Map<Integer, Map<Integer, List<Map>>> subtableData = itemMessage.getSubtableData();
        Assert.assertNotNull(subtableData);
        Assert.assertEquals(subtableData.size(), 6);
        Assert.assertNotNull(subtableData.get(1));
        Assert.assertEquals(subtableData.get(1).get(191).size(), 8);
        List<Map> Attr191 = subtableData.get(1).get(191);
        Assert.assertEquals(Attr191.get(0).get("AttributeID"), "191");
        Assert.assertEquals(Attr191.get(0).get("CSItemTableID"), "19");
        Assert.assertEquals(Attr191.get(0).get("ClassID"), "192");
        Assert.assertEquals(Attr191.get(0).get("ItemID"), "62");
        Assert.assertEquals(Attr191.get(0).get("ItemType"), "Pdmarticle");
        Assert.assertEquals(Attr191.get(0).get("LanguageID"), "1");
        Assert.assertEquals(Attr191.get(0).get("SortOrder"), "0");
        Assert.assertEquals(Attr191.get(0).get("SubItemID"), "62");
        Assert.assertEquals(Attr191.get(0).get("200:Value"), "60.00");
        Assert.assertEquals(Attr191.get(0).get("200:FormattedValue"), "60.00");
        Assert.assertEquals(Attr191.get(0).get("197:Value"), "1");
        Assert.assertEquals(Attr191.get(0).get("197:FormattedValue"), "1");
        Assert.assertEquals(Attr191.get(0).get("196:Value"), "3.00");
        Assert.assertEquals(Attr191.get(0).get("196:FormattedValue"), "3.00");
        Assert.assertEquals(Attr191.get(0).get("194:Value"), "3719");
        Assert.assertEquals(Attr191.get(0).get("194:FormattedValue"), "All");
        Assert.assertEquals(Attr191.get(0).get("193:Value"), "80.00");
        Assert.assertEquals(Attr191.get(0).get("193:FormattedValue"), "80.00");
        Assert.assertEquals(Attr191.get(0).get("189:Value"), (long) 20);
        Assert.assertEquals(Attr191.get(0).get("189:FormattedValue"), "20 %");
        Assert.assertEquals(Attr191.get(0).get("188:Value"), "3346");
        Assert.assertEquals(Attr191.get(0).get("188:FormattedValue"), "Germany");
        Assert.assertEquals(Attr191.get(0).get("187:Value"), "3589");
        Assert.assertEquals(Attr191.get(0).get("187:FormattedValue"), "Euro");
        Assert.assertEquals(Attr191.get(0).get("186:Value"), (long) 0);
        Assert.assertEquals(Attr191.get(0).get("186:FormattedValue"), "0 %");
        Assert.assertEquals(Attr191.get(0).get("185:Value"), "Standard");
        Assert.assertEquals(Attr191.get(0).get("185:FormattedValue"), "Standard");
    }


    private ClassLoader getClassLoader() {

        return getClass().getClassLoader();
    }


}