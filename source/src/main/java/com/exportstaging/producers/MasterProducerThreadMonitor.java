package com.exportstaging.producers;

import com.exportstaging.connectors.idbconnector.IntermediateDAO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.List;

import static com.exportstaging.common.ExportMiscellaneousUtils.*;


public class MasterProducerThreadMonitor implements ApplicationContextAware {

    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    private IntermediateDAO intermediateDAO;

    @Value("${mysql.prefix.exportstaging}")
    private String idbTablePrefix;
    @Value("${mysql.csdbprefix.name}")
    private String mCsDbPrefix;

    private List<Producer> producerBeans;
    private MasterProducerExecutor masterProducerExecutor = null;

    private final static Logger logger = LogManager.getLogger("exportstaging");

    /**
     * Manage the @{@link com.exportstaging.moderators.BatchModerator} thread based on intermediate database tables and current running threads.
     */
    public void manageProducerThreads() {
        String producerName = "";
        try {
            for (Producer producerBean : getProducerBeans()) {
                producerName = producerBean.getProducerName();
                startNewThreads(producerBean);
                removeOldThreads(producerBean);
            }
        } catch (Exception e) {
            logger.error("[" + producerName + " ]:Exception while managing Producer threads. " + e.getMessage() +
                    TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        masterProducerExecutor = (MasterProducerExecutor) applicationContext.getBean("masterProducerExecutor");
    }

    private List<Producer> getProducerBeans() {
        return producerBeans;
    }

    public void setProducerBeans(List<Producer> producerBeans) {
        this.producerBeans = producerBeans;
    }

    /**
     * Starting new @{@link com.exportstaging.moderators.BatchModerator} thread based on idb tables and current running threads
     *
     * @param producerBean Bean of Producer for which new thread will start
     */
    private void startNewThreads(Producer producerBean) {
        List<String> itemTypeFromRunningThreads = masterProducerExecutor.getRunningThreads(producerBean.getProducerName());
        List<String> itemTypesFromMappingTables = intermediateDAO.getItemTypesFromMappingTable(producerBean.getProducerID());
        itemTypesFromMappingTables.removeAll(itemTypeFromRunningThreads);
        if (itemTypesFromMappingTables.size() > 0) {
            masterProducerExecutor.addThreads(producerBean, itemTypesFromMappingTables);
        }
    }

    /**
     * Removing old @{@link com.exportstaging.moderators.BatchModerator} thread based on idb tables and current running threads
     *
     * @param producerBean Bean of Producer for which old thread will remove
     */
    private void removeOldThreads(Producer producerBean) {
        String producerName = producerBean.getProducerName();
        List<String> itemTypeFromRunningThreads = masterProducerExecutor.getRunningThreads(producerName);
        List<String> itemTypesFromMappingTables = intermediateDAO.getItemTypesFromMappingTable(producerBean.getProducerID());
        itemTypeFromRunningThreads.removeAll(itemTypesFromMappingTables);
        if (itemTypeFromRunningThreads.size() > 0) {
            masterProducerExecutor.removeThreads(producerName, itemTypeFromRunningThreads);
            intermediateDAO.dropIntermediateTableForItemType(itemTypeFromRunningThreads);
        }
    }

    private void logError(String message) {
        logger.error("[IDB_TableMonitor]: " + message);
    }

    private void logInfo(String message, boolean sendToConsole) {
        String log = "[IDB_TableMonitor]: " + message;
        logger.info(log);
        if (sendToConsole) {
            System.out.println(log);
        }
    }

    private void logWarn(String message) {
        logger.warn("[IDB_TableMonitor]: " + message);
    }
}