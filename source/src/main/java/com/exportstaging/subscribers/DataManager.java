package com.exportstaging.subscribers;

import com.exportstaging.common.ExportMiscellaneousUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.exportstaging.common.ExportMiscellaneousUtils.*;

@Component
public class DataManager {
    @Value("${core.project.name}")
    protected String projectName;
    @Value("${mysql.type.item}")
    protected String mTypeItem;
    @Value("${mysql.type.configuration}")
    protected String mTypeConfiguration;
    @Value("${mysql.type.filecontent}")
    protected String mTypeFileContent;
    @Value("${mysql.type.mapping}")
    protected String mTypeMapping;
    @Value("${mysql.column.id}")
    private String mID;
    @Value("${mysql.column.action}")
    private String mAction;
    @Value("${mysql.column.jmspriority}")
    protected String mJmsPriority;
    @Value("${mysql.column.inserttime}")
    protected String mInsertTime;
    @Value("${mysql.action.failed}")
    protected int mActionFailed;
    @Value("${mysql.action.update}")
    private int mActionUpdate;
    @Value("${mysql.action.delete}")
    private int mActionDelete;
    @Value("${mysql.action.create}")
    private int mActionCreate;
    @Value("${export.data.folder.root}")
    private String rootFolder;
    @Value("${activemq.subscriber.master}")
    String masterSubscriber;
    @Value("${activemq.subscriber.elastic}")
    String elasticSubscriber;
    private String initialCacheFilesPath;
    private String serializedFilesPath;
    private boolean isProjectSubscriber;
    private final static Logger logger = LogManager.getLogger("exportstaging");
    private volatile ConcurrentHashMap<String, Long> itemInsertTimeHashMap = null;
    private volatile long cleanUpTime = 0;

    private DataManager() {
    }

    boolean isMessageValid(MapMessage message) {
        try {
            int action = message.getInt(mAction);
            String mapKey = generateItemMapKey(message);
            long insertTime = message.getLong("IDBInsertTime");
            return action == mActionDelete || action == mActionCreate ||
                    (itemInsertTimeHashMap.containsKey(mapKey) && itemInsertTimeHashMap.get(mapKey) == insertTime);
        } catch (JMSException e) {
            return false;
        }

    }

    private String generateItemMapKey(MapMessage message) throws JMSException {
        String id = message.getString(mID);
        String itemType = message.getString(ExportMiscellaneousUtils.CONSTANT_ITEM_TYPE);
        String type = message.getString(ExportMiscellaneousUtils.CONSTANT_TYPE);
        String mapKey = itemType;
        if (type.equals(mTypeFileContent)) {
            mapKey += "_" + type;
        }
        mapKey += "_" + id;
        return mapKey;
    }


    void updateData(MapMessage message, String subscriberName) {
        try {
            int action = message.getInt(mAction);
            String type = message.getString(ExportMiscellaneousUtils.CONSTANT_TYPE);
            if (type.equals(mTypeConfiguration) || type.equals(mTypeMapping)) {
                return;
            }
            if (itemInsertTimeHashMap == null) {
                loadMap(subscriberName);
            }
            String mapKey = generateItemMapKey(message);
            long insertTime = message.getLong("IDBInsertTime");
            if (action == mActionDelete) {
                itemInsertTimeHashMap.put(mapKey, (long) -1);
            } else {
                if (itemInsertTimeHashMap.containsKey(mapKey)) {
                    if (itemInsertTimeHashMap.get(mapKey) != -1) {
                        if (insertTime > itemInsertTimeHashMap.get(mapKey)) {
                            itemInsertTimeHashMap.put(mapKey, insertTime);
                        }
                    }
                } else {
                    itemInsertTimeHashMap.put(mapKey, insertTime);
                }
            }
        } catch (JMSException e) {
            String errorMessage = "JMSException while maintaining data entries. Error Message: ";
            logError(errorMessage, e);
        } catch (Exception e) {
            String errorMessage = "Exception while maintaining data entries. Error Message: ";
            logError(errorMessage, e);
        }
    }

    boolean loadMap(String subscriberName) {
        if (!subscriberName.equals("coreSubscriber")) {
            isProjectSubscriber = true;
        }
        if (initialCacheFilesPath == null) {
            createProjectPath(subscriberName);
        }
        File file;
        if ((file = new File(serializedFilesPath)).exists()) {
            return loadDataFile(file);
        } else if ((file = new File(serializedFilesPath.replace(".data", ".ser"))).exists()) {
            return loadSerializedFile(file);
        } else {
            if (itemInsertTimeHashMap == null) {
                itemInsertTimeHashMap = new ConcurrentHashMap<>();
            }
        }
        return true;
    }

    void clearMap(long insertTime, String subscriberName) {
        if (cleanUpTime >= insertTime) {
            //True, because cleanup is already performed by another subscriber. So no need to cleanup again.
            return;
        }
        cleanUpTime = insertTime;
        if (itemInsertTimeHashMap != null) {
            itemInsertTimeHashMap = new ConcurrentHashMap<>();
        }
        //Deleting existing file because it is no longer needed
        if (serializedFilesPath == null) {
            createProjectPath(subscriberName);
        }
        deleteFile(new File(serializedFilesPath));
    }

    void removeItemType(List<String> itemTypes) {
        if (itemInsertTimeHashMap != null)
            itemInsertTimeHashMap.keySet().stream().filter(mapKey -> itemTypes.contains(mapKey.split("_")[0])).forEach(mapKey -> itemInsertTimeHashMap.remove(mapKey));
    }

    private void createProjectPath(String subscriberName) {
        initialCacheFilesPath = ExportMiscellaneousUtils.getDataFolderPath(projectName);
        String subsName = "coreSubscriber";
        if (!subscriberName.equals(masterSubscriber) && !subscriberName.equals(elasticSubscriber)) {
            subsName = subscriberName;
        }
        serializedFilesPath = initialCacheFilesPath + File.separator + subsName + ".data";
    }


    @PreDestroy
    private void destroy() {
        try {
            if (serializedFilesPath != null) {
                File file = new File(serializedFilesPath);
                if (!file.exists()) {
                    if (file.getParentFile().mkdirs()) {
                        boolean newFile = file.createNewFile();
                    }
                }
                
                if(itemInsertTimeHashMap != null) {
                    new ObjectMapper().writeValue(new File(serializedFilesPath), itemInsertTimeHashMap);
                    logger.info("[DataManager] Data Cached successfully for next run");
                }
                
                deleteFile(new File(serializedFilesPath.replace(".data", ".ser")));
            }
        } catch (IOException e) {
            String errorMessage = "[DataManager] Failed to cache data for data management";
            logError(errorMessage, e);
        }
    }

    private boolean loadSerializedFile(File file) {
        try {
            ObjectInputStream stream = new ObjectInputStream(new FileInputStream(file));
            itemInsertTimeHashMap = (ConcurrentHashMap<String, Long>) stream.readObject();
            stream.close();
        } catch (ClassNotFoundException | IOException exception) {
            logError("Exception in loadSerializedFile.", exception);
            return false;
        }
        return true;
    }

    private boolean loadDataFile(File file) {
        try {
            itemInsertTimeHashMap = getHashMapFromJSONFile(file);
        } catch (IOException e) {
            logError("[DataManager] IOException while loading CSV file. ", e);
            return false;
        }
        return true;
    }

    private void deleteFile(File file) {
        try {
            if (file != null && file.exists() && file.delete())
                logger.info("[DataManager] Successfully deleted " + file.getName());
        } catch (SecurityException e) {
            logError("[DataManager] SecurityException while deleting " + file.getName() + ". ", e);
        }
    }

    /**
     * Converts single json object from the file to ConcurrentHashMap. Note that the file should contain [String, LONG] structure and a single json.
     *
     * @param file {@link File} object which contains the json to be read from
     * @return Converted HashMap from the source file
     * @throws IOException In case of Permissions issues or file is missing.
     */
    private ConcurrentHashMap<String, Long> getHashMapFromJSONFile(File file) throws IOException {
        return new ObjectMapper().readValue(file, new TypeReference<ConcurrentHashMap<String, Long>>() {
        });
    }

    private void logError(String message, Exception e) {
        message = message + " " + e.getMessage();
        logger.error(message);
        logger.debug(message + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
    }
}