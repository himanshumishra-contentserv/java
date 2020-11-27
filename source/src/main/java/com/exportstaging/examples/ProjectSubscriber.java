package com.exportstaging.examples;

import com.exportstaging.domain.ConfigurationMessage;
import com.exportstaging.domain.ItemMessage;
import com.exportstaging.domain.Message;
import com.exportstaging.subscribers.AbstractCustomSubscriber;

import java.util.*;

public class ProjectSubscriber extends AbstractCustomSubscriber {

    public static void main(String[] args) {
        ProjectSubscriber projectSubscriber = new ProjectSubscriber();
        projectSubscriber.startSubscriber();
    }

    @Override
    public List<String> getHandledItemTypes() {
        List<String> list = new ArrayList<>();
        System.out.println("Provide object types to export separated by comma:");
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();
        list.addAll(Arrays.asList(input.split(",")));
        return list;
    }

    @Override
    public void processMessage(ConfigurationMessage configurationMessage, String itemType, String type) {
        String messageType = configurationMessage.getMessageType();
        switch (messageType) {
            case "Configuration":
                Map<String, String> configurationData = configurationMessage.getConfigurationData();
                for (String key : configurationData.keySet()) {
                    System.out.println(key + ": " + configurationData.get(key));
                }
                break;
            case "Mapping":
                List<String> mappingData = configurationMessage.getMappingData();
                for (String str : mappingData) {
                    System.out.println(str);
                }
                break;
        }
    }

    @Override
    public void processMessage(ItemMessage itemMessage, String itemType, String type) {
        Map<String, Map<String, String>> itemData = itemMessage.getItemData();
        for (String languageID : itemData.keySet()) {
            System.out.println(languageID + " => " + itemData.get(languageID));
        }

        Map<Integer, Map<Integer, List<Map>>> referenceData = itemMessage.getReferenceData();
        for (int LanguageID : referenceData.keySet()) {
            for (int AttributeID : referenceData.get(LanguageID).keySet()) {
                for (Map refData : referenceData.get(LanguageID).get(AttributeID)) {
                    for (Object key : refData.keySet()) {
                        System.out.println(LanguageID + " => " + AttributeID + " => " + key + " => " + refData.get(key));
                    }
                }
            }
        }
        Map<Integer, Map<Integer, List<Map>>> subtableData = itemMessage.getSubtableData();
        for (int LanguageID : subtableData.keySet()) {
            for (int AttributeID : subtableData.get(LanguageID).keySet()) {
                for (Map subData : subtableData.get(LanguageID).get(AttributeID)) {
                    for (Object key : subData.keySet()) {
                        System.out.println(LanguageID + " => " + AttributeID + " => " + key + " => " + subData.get(key));
                    }
                }
            }
        }
    }

    @Override
    public void processMessage(Message message, String itemType, String type) {
        Map<String, Map<String, String>> msg = message.getItemData();
        for (String key : msg.keySet()) {
            System.out.println(key + ": " + msg.get(key));
        }
    }

    @Override
    public void deleteMessage(List<String> ids, String itemType, String type) {
        System.out.println("ID: " + ids + " Item Type: " + itemType + " Module Type: " + type);
    }

    @Override
    public boolean operationCleanup() {
        return super.operationCleanup();
    }

    @Override
    public boolean operationInitialize(List<String> itemTypes) {
        System.out.println("Call for initial export with item types:" + itemTypes);
        return super.operationInitialize(itemTypes);
    }

    @Override
    protected Boolean isDurableSubscriber() {
        return false;
    }

    @Override
    public boolean loggerDebugMode(String level) {
        System.out.printf("Debug Logger mode level changed to : " + level);
        return super.loggerDebugMode(level);
    }
}