package com.exportstaging.examples;

import com.exportstaging.api.ExternalItemAPI;
import com.exportstaging.api.ItemAPIs;
import com.exportstaging.api.RecordAPIs;
import com.exportstaging.api.domain.*;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.api.resultset.ItemsResultSet;
import com.exportstaging.api.searchfilter.SearchFilterCriteria;
import com.exportstaging.api.wraper.ItemWrapper;
import com.exportstaging.api.wraper.RecordWrapper;
import com.exportstaging.utils.ExportCassandraConfigurator;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component
public class ExportAPITest {

    public static void main(String[] args) {
        ExportAPITest exportAPITest = new ExportAPITest();
        exportAPITest.execute();
    }

    public void execute() {
        ItemAPIs itemAPI = null;
        RecordAPIs recordAPI = null;
        ItemWrapper itemWrapper = null;
        new ExportCassandraConfigurator("localhost", "keyspace_name");
        try {
            recordAPI = new ExternalItemAPI(RecordAPIs.ITEM_TYPE_WORKFLOW);
            RecordWrapper recordWrapper = recordAPI.getItemByStateID(122);
            System.out.println("Record ID: " + recordWrapper.getItemID());

            itemAPI = new ExternalItemAPI(ExternalItemAPI.ITEM_TYPE_PRODUCT);
            itemWrapper = itemAPI.getItemByExternalKey("CS-62");
            System.out.println("ItemID: " + itemWrapper.getItemID());

            SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria();
            searchFilterCriteria.setLanguageId("2");

            Set<Long> itemIDs = itemAPI.getItemIdsByFilters(searchFilterCriteria);
            System.out.println(itemIDs.size());

            ItemsResultSet itemsResultSet = itemAPI.getItemsByFilter(searchFilterCriteria);
            System.out.println(itemsResultSet.count());

            ItemWrapper itemWrapper1;
            while ((itemWrapper1 = itemsResultSet.nextItem()) != null) {
                System.out.println(itemWrapper1.getItemID());
            }

            ItemsResultSet itemsResultSet1 = itemAPI.getItemIdByFilterFromMaterializeView(searchFilterCriteria);
            ItemWrapper itemWrapper2;
            while ((itemWrapper2 = itemsResultSet1.nextItem()) != null) {
                System.out.println(itemWrapper2.getItemID());
            }

            Attribute attribute = itemAPI.getAttributeByID(332);
            System.out.println("Attribute ID: " + attribute.getId());
            System.out.println("Attribute Label: " + attribute.getLabel());

            Attribute attribute1 = itemAPI.getAttributeByExternalKey("HEIGHT");
            System.out.println("Attribute ID: " + attribute.getId());
            System.out.println("Attribute Label: " + attribute.getLabel());

            System.out.println(itemAPI.getAllFields());
            System.out.println(itemAPI.getCustomFields());
            System.out.println(itemAPI.getPluginFields());
            System.out.println(itemAPI.getStandardFields());
            System.out.println(itemAPI.getAttributeIDsByClassID(248));
            System.out.println(itemAPI.getLanguagesShortName());

            itemsResultSet = itemAPI.getItemsByFilter(searchFilterCriteria);
            List languageIDs = new ArrayList();
            languageIDs.add(1);
            languageIDs.add(2);
            ItemWrapper wrapperImpl;
            while ((wrapperImpl = itemsResultSet.nextItem(languageIDs)) != null) {
                System.out.println("Label: " + wrapperImpl.getValue("Label", "en"));
                System.out.println("ClassMapping: " + wrapperImpl.getClassMapping());
                List<Long> attributeIDs = wrapperImpl.getAttributeIDs();
                System.out.println(attributeIDs);
                for (long attributeID : attributeIDs) {
                    System.out.println("ID: " + attributeID + " Value: " + wrapperImpl.getValue(String.valueOf(attributeID)) +
                            " FormattedValue: " + wrapperImpl.getFormattedValue(String.valueOf(attributeID)));
                }
                ExportValues exportValues = wrapperImpl.getValues("en");
                printSubtable(exportValues.getSubtableList(), 1);
            }
            itemAPI.close();
        } catch (ExportStagingException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (itemAPI != null) itemAPI.close();
        }
    }

    private void printSubtable(List<Subtable> subtableList, int level) {
        for (Subtable subtable : subtableList) {
            for (int i = 0; i < level - 1; i++) {
                System.out.print("\t");
            }
            System.out.println("ID: " + subtable.getAttributeID());
            ExportValues exportValues = subtable.getExportValues();
            for (AttributeValue attribute : exportValues.getAttributeList()) {
                for (int i = 0; i < level; i++) {
                    System.out.print("\t");
                }
                System.out.println("ID: " + attribute.getId() + " Value: " + attribute.getValue() + " FormattedValue: "
                        + attribute.getFormattedValue());
            }

            for (Reference reference : exportValues.getReferenceList()) {
                for (int i = 0; i < level; i++) {
                    System.out.print("\t");
                }
                System.out.println("ReferenceID: " + reference.getReferenceID() + " ID: " + reference.getAttributeID() + " RefValue[13]: " + reference.getValueById(13)
                        + " RefFormattedValue[13]: " + reference.getFormattedValueById(13));
            }
            printSubtable(exportValues.getSubtableList(), level + 1);
        }
    }
}
