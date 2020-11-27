package com.exportstaging.subscribers;

import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.domain.ItemMessage;
import com.exportstaging.domain.Message;
import cs.sockets.websocket.CSWebSocketMessage;
import cs.sockets.websocket.CSWebSocketMessageArrayValue;
import cs.sockets.websocket.CSWebSocketMessageObjectValue;
import cs.sockets.websocket.CSWebSocketServer;
import org.apache.commons.lang3.StringUtils;

import java.net.ServerSocket;
import java.util.*;

public class WebSocketSubscriber extends AbstractSubscriber {

    private CSWebSocketServer server = null;

    private static int WEBSOCKET_PORT = 8080;

    Map<String, WebsocketServerMessageDispatcher> dispatcherList;

    public void startSubscriber() {
        dispatcherList = new HashMap<String, WebsocketServerMessageDispatcher>();

        Thread thread = new WebSocketServerStarter();

        thread.start();
    }

    @Override
    public List<String> getHandledItemTypes() {
        ArrayList<String> types = new ArrayList<String>();
        types.add("Pdmarticle");
        types.add("Mamfile");
        types.add("Pdmarticlestructure");
        types.add("User");
        types.add("Translationjob");
        types.add("Opensearcharea");
        types.add("Opensearchareamapping");
        types.add("Task");

        return types;
    }

    @Override
    public Boolean deleteMessage(List<String> ids, String itemType, String type, String projectName) {
        if (server == null) {
            return true;
        }

        CSWebSocketMessage message = null;
        CSWebSocketMessageObjectValue rootItem = null;

        for (String id : ids) {
            message = new CSWebSocketMessage();
            rootItem = new CSWebSocketMessageObjectValue();

            message.add("type", 2);

            rootItem = message.addObject("record");
            rootItem.add("Class", itemType);
            rootItem.add("BaseClass", type);
            rootItem.add("ID", id);

            dispatchMessage(message, itemType, type, projectName);
        }

        // TODO Auto-generated method stub
        return super.deleteMessage(ids, itemType, type, projectName);
    }

    @Override
    public Boolean processMessage(Message message, String itemType, String type, String projectName) {
        if (server == null) {
            return true;
        }

        int action = message.getAction();

        if ((action < 1) || (action > 3)) {
            return true;
        }

        Map<String, Map<String, String>> itemData = message.getItemData();
        CSWebSocketMessage socketMessage = new CSWebSocketMessage();
        CSWebSocketMessageObjectValue rootItem = null;
        Map<String, String> item = null;

        socketMessage.add("type", action);

        rootItem = socketMessage.addObject("record");
        rootItem.add("Class", itemType);
        rootItem.add("BaseClass", type);

        for (Map.Entry<String, Map<String, String>> language : itemData.entrySet()) {
            item = language.getValue();

            for (HashMap.Entry<String, String> entry : item.entrySet()) {
                String key = entry.getKey();
                String value = String.valueOf(entry.getValue());

                if (value == null) {
                    rootItem.addNull(key);
                } else {
                    rootItem.add(key, value);
                }
            }
        }

        dispatchMessage(socketMessage, itemType, type, projectName);

        return super.processMessage(message, itemType, type, projectName);
    }

    @Override
    public Boolean processMessage(ItemMessage itemMessage, String itemType, String type, String projectName) {
        if (server == null) {
            return true;
        }

        int action = itemMessage.getAction();

        if ((action < 1) || (action > 3)) {
            return true;
        }

        Map<String, Map<String, String>> itemData = itemMessage.getItemData();
        CSWebSocketMessage socketMessage = new CSWebSocketMessage();
        CSWebSocketMessageObjectValue languageItem = null;
        CSWebSocketMessageObjectValue rootItem = null;
        Map<String, String> item = null;

        socketMessage.add("type", action);

        rootItem = socketMessage.addObject("record");
        rootItem.add("Class", itemType);
        rootItem.add("BaseClass", type);

        ArrayList<String> languageFields = new ArrayList<String>(
                Arrays.asList(new String[]{"Label", "WorkflowID", "StateID"})
        );

        for (Map.Entry<String, Map<String, String>> language : itemData.entrySet()) {
            item = language.getValue();
            languageItem = rootItem.addObject(language.getKey());

            for (HashMap.Entry<String, String> entry : item.entrySet()) {
                String key = entry.getKey();
                String value = String.valueOf(entry.getValue());

                if (
                        key.matches("-?[0-9]+\\:(Formatted)?Value")
                                || languageFields.contains(key)
                ) {
                    if (value == null) {
                        languageItem.addNull(key);
                    } else {
                        languageItem.add(key, value);
                    }
                } else if (!key.substring(0, 1).equals("_")) {
                    if (value == null) {
                        rootItem.addNull(key);
                    } else {
                        if (key.equals("ClassMapping")) {
                            CSWebSocketMessageArrayValue classMapping = rootItem.addArray(key);

                            String[] classIds = value.trim().split(" ");

                            for (String classId : classIds) {
                                classMapping.add(classId);
                            }
                        } else {
                            rootItem.add(key, value);
                        }
                    }
                }
            }
        }

        dispatchMessage(socketMessage, itemType, type, projectName);

        return super.processMessage(itemMessage, itemType, type, projectName);
    }

    protected Boolean isDurableSubscriber() {
        return false;
    }

    @Override
    protected Boolean isHandlingInitialExport() {
        return false;
    }

    private Boolean init() {
        if (startSocket() == false) {
            return false;
        }

        List<String> exportDbNames = null;
        List<String> projectNames  = null;
        try {
            Map<String, String> projectDetails = ExportMiscellaneousUtils.getProjectDetails();
            exportDbNames = new ArrayList<>(projectDetails.keySet());
            projectNames  = new ArrayList<>(projectDetails.values());
        } catch (ExportStagingException e) {
            logger.warn("[" + subscriberName + "] " + e.getMessage());
        }

        this.setExportDbNames(exportDbNames);

        String projectList = StringUtils.join(projectNames, ",");

        try {
            super.startSubscriber();
        } catch (CannotCreateConnectionException ignored) {
        }

        log("Created socket for projects: " + projectList);

        return true;
    }

    private Boolean startSocket() {
        try {
            (new ServerSocket(WEBSOCKET_PORT)).close();

            server = new CSWebSocketServer(WEBSOCKET_PORT);

            server.start();
        } catch (Exception e) {
            // socket server already running
            return false;
        }

        return true;
    }

    private void dispatchMessage(CSWebSocketMessage message, String itemType, String type, String projectName) {
        String key = projectName + "_" + itemType;

        WebsocketServerMessageDispatcher dispatcher = dispatcherList.get(key);

        if (
                dispatcher == null
                        || !dispatcher.isAlive()
        ) {
            dispatcher = new WebsocketServerMessageDispatcher(projectName, itemType, type);

            dispatcher.addMessage(message);

            dispatcherList.put(key, dispatcher);

            dispatcher.start();
        } else {
            dispatcher.addMessage(message);
        }
    }

    private class WebSocketServerStarter extends Thread {

        public void run() {
            try {
                while (init() == false) {
                    // check every 10 seconds whether the websocket is still opened by another subscriber
                    Thread.sleep(10000);
                }

                // watch websocket to make sure it has not been closed
                while (true) {
                    startSocket();
                    Thread.sleep(10000);
                }
            } catch (InterruptedException e) {
                //parent thread interrupted, return
                return;
            }
        }
    }

    private class WebsocketServerMessageDispatcher extends Thread {

        private ArrayList<CSWebSocketMessage> messages;

        private String projectName;

        private String itemType = "";

        private String type = "";

        private long lastChange = 0;

        public WebsocketServerMessageDispatcher(String projectName, String itemType, String type) {
            messages = new ArrayList<>();

            this.projectName = projectName;
            this.itemType = itemType;
            this.type = type;
        }

        public void run() {
            ArrayList<CSWebSocketMessage> messageList = new ArrayList<CSWebSocketMessage>();
            ;

            while (true) {
                if (
                        lastChange < System.currentTimeMillis() - 5000
                                || messages.size() >= 100
                ) {
                    synchronized (messages) {
                        messageList.addAll(messages);

                        messages.clear();
                    }

                    for (CSWebSocketMessage message : messageList) {
                        if (messageList.size() > 3) {
                            message = this.getMassUpdateMessage();
                            server.sendMessage(message, projectName);
                            break;
                        }
                        server.sendMessage(message, projectName);
                    }

                    messageList.clear();
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // thread interrupted, return
                    return;
                }
            }
        }

        public void addMessage(CSWebSocketMessage message) {
            synchronized (messages) {
                messages.add(message);
            }

            lastChange = System.currentTimeMillis();
        }

        private CSWebSocketMessage getMassUpdateMessage() {
            CSWebSocketMessage message = new CSWebSocketMessage();
            CSWebSocketMessageObjectValue rootItem = null;

            // action type for mass updates
            message.add("type", -1);

            rootItem = message.addObject("record");
            rootItem.add("Class", itemType);
            rootItem.add("BaseClass", type);

            return message;
        }

    }

}