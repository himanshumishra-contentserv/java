package com.exportstaging.producers;

import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.connectors.idbconnector.IntermediateDAO;
import com.exportstaging.moderators.BatchModerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

import static com.exportstaging.common.ExportMiscellaneousUtils.*;

public class MasterProducerExecutor {

    private final static Logger logger = LogManager.getLogger("exportstaging");
    @Autowired
    private ClassPathXmlApplicationContext applicationContext;
    @Autowired
    private IntermediateDAO intermediateDAO;

    @Value("${export.thread.sleep.time}")
    private int threadSleepTime;
    @Value("${activemq.producer.master}")
    private String masterProducer;
    @Value("${activemq.producer.project}")
    private String projectProducer;
    @Value("${data.json.file.suffix.headers}")
    private String headers;

    private volatile ConcurrentHashMap<String, Map<String, ScheduledFuture>> threadedMap = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, Map<String, BatchModerator>> threadedBatchModeratorMap = new ConcurrentHashMap<>();

    public void addThreads(Producer producerBean, List<String> itemTypes) {
        String producerName = producerBean.getProducerName();
        threadedMap.putIfAbsent(producerName, new HashMap<>());
        threadedBatchModeratorMap.putIfAbsent(producerName, new HashMap<>());
        if (!threadedMap.get(producerName).containsKey(ExportMiscellaneousUtils.EXPORT_TYPE_OPERATION)) {
            startThread(producerBean, ExportMiscellaneousUtils.EXPORT_TYPE_OPERATION);
        }
        itemTypes.remove(headers);
        for (String itemType : itemTypes) {
            startThread(producerBean, itemType);
        }
    }

    private void startThread(Producer producerBean, String itemType) {
        String producerName = "";
        try {
            producerName = producerBean.getProducerName();
            if (!threadedMap.get(producerName).containsKey(itemType)) {
                //Managing producer threads bases on subscriber item mapping table
                int producerIdsSum = intermediateDAO.getProducerIdSumFromMappingTable(itemType);
                if ((producerIdsSum == -1) || ((producerIdsSum == Producer.masterProducerID && projectProducer.equalsIgnoreCase(producerName))
                        || (producerIdsSum == Producer.projectProducerID && masterProducer.equalsIgnoreCase(producerName)))) {
                    return;
                }
                reinitializeDeleteStatusValue(producerName, itemType);

                String loggerInfo = "[" + producerName + "] : Starting thread of " + producerName + " for " + itemType;
                System.out.println(loggerInfo);
                logger.info(loggerInfo);
                BatchModerator batchModerator = (BatchModerator) applicationContext.getBean("batchModerator");
                batchModerator.setItemType(itemType);
                batchModerator.setProducer(producerBean);
                batchModerator.setDeleteStatus(producerIdsSum);

                //The following if condition is to wait for operation data to be exported before starting threads of others types
                if (itemType.equals(ExportMiscellaneousUtils.EXPORT_TYPE_OPERATION)) {
                    threadedMap.get(producerName).put(itemType, producerBean.getTaskScheduler().scheduleWithFixedDelay(batchModerator, 2000));
                    threadedBatchModeratorMap.get(producerName).put(itemType, batchModerator);
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        String message = "[" + producerName + "]:Interruption while creating threads for Core Types. Error Message:" + e.getMessage();
                        logger.error(message);
                        logger.debug(message + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
                        Thread.currentThread().interrupt();
                    }
                    return;
                }
                threadedMap.get(producerName).put(itemType, producerBean.getTaskScheduler().scheduleWithFixedDelay(batchModerator, threadSleepTime));
                threadedBatchModeratorMap.get(producerName).put(itemType, batchModerator);
            }
        } catch (Exception e) {
            String message = "Exception while starting thread for " + itemType + ". Error Message:" + e.getMessage();
            logger.error(message);
            logger.debug(message + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
        }
    }

    public void removeThreads(String producerName, List<String> itemTypes) {
        for (String itemType : itemTypes) {
            reinitializeDeleteStatusValue(producerName, itemType);
            String loggerInfo = "[" + producerName + "] : Removing thread of " + producerName + " for " + itemType;
            System.out.println(loggerInfo);
            logger.info(loggerInfo);
            ScheduledFuture thread = threadedMap.get(producerName).get(itemType);
            if (thread != null) {
                thread.cancel(false);
            }
            threadedMap.get(producerName).remove(itemType);
            threadedBatchModeratorMap.get(producerName).remove(itemType);
        }
    }

    public List<String> getRunningThreads(String producerName) {
        threadedMap.putIfAbsent(producerName, new HashMap<>());
        threadedBatchModeratorMap.putIfAbsent(producerName, new HashMap<>());
        return new ArrayList<>(threadedMap.get(producerName).keySet());
    }

    /**
     * Method will get called when new thread added or removed to reinitialise the deleteStatus value
     *
     * @param currentProducerName Producer name for which thread is added or removed.
     * @param itemType            Item type for which batchmoderator thread is started or removed.
     */
    private void reinitializeDeleteStatusValue(String currentProducerName, String itemType) {
        if (masterProducer.equalsIgnoreCase(currentProducerName)) {
            currentProducerName = projectProducer;
        } else {
            currentProducerName = masterProducer;
        }
        threadedBatchModeratorMap.putIfAbsent(currentProducerName, new HashMap<>());
        if (threadedBatchModeratorMap.get(currentProducerName).containsKey(itemType)) {
            BatchModerator batchModerator = threadedBatchModeratorMap.get(currentProducerName).get(itemType);
            int previousStatusValue = batchModerator.getDeleteStatus();
            batchModerator.setDeleteStatus(intermediateDAO.getProducerIdSumFromMappingTable(itemType));
            logger.info("[" + currentProducerName + "] Delete status value is updated for " + itemType + " thread  from " + previousStatusValue + " to " + batchModerator.getDeleteStatus());
        }
    }
}