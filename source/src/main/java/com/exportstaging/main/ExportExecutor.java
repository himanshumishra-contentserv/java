package com.exportstaging.main;

import com.exportstaging.activemq.ExportActiveMQUtils;
import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.elasticsearch.updatedatamodel.ElasticIndexUpdater;
import com.exportstaging.initial.ExportInitializerUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.*;

public class ExportExecutor {

    private static Properties properties;
    private static String projectName;
    private static boolean printSubscriberInfo = true;
    private static ExportActiveMQUtils exportActiveMQUtils;
    private static String projectPropertiesPath;
    private static Logger logger;
    private static String exportDbName;

    public static void main(String[] startUpSettings) {
        try {
            if (isElasticIndexUpdater(startUpSettings)) {
                configureProjectPathProperty(startUpSettings);
                loadElasticSpecificContext(startUpSettings);
            } else {
                if (startUpSettings.length > 0 && startUpSettings[0].startsWith("-")) {
                    getStartUpConfigurations(startUpSettings);
                }
                configureProjectPathProperty(startUpSettings);
                loadApplicationContext();
                if (!ExportMiscellaneousUtils.isExecutorRunningExternally(projectName)) {
                    getExportExecutorProcessId();
                }
                while (printSubscriberInfo) {
                    try {
                        Thread.sleep(Integer.parseInt(properties.getProperty("export.uptime.delay")));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    exportActiveMQUtils.printQueueSubscriberInfo(exportDbName);
                }
            }
        } catch (Exception e) {
            logger.error("Exception while starting ExportExecutor: " + e.getMessage());
        }
    }


    private static void getStartUpConfigurations(String[] startUpSettings) throws ExportStagingException {
        if (startUpSettings.length > 0) {
            for (String startUpParameter : startUpSettings) {
                if (!startUpParameter.startsWith("-")) {
                    throw new ExportStagingException("Invalid Startup Parameter " + startUpParameter);
                }
                String[] options = startUpParameter.split(":", 2);
                String settingsKey = options[0];
                String settingsValue = options[1];
                prepareStartupParam(settingsKey, settingsValue);
            }
        }
    }


    private static void prepareStartupParam(String settingsKey, String settingsValue) throws ExportStagingException {
        switch (settingsKey) {
            case ExportMiscellaneousUtils.EXPORT_PROJECT_NAME: {
                projectName = settingsValue;
                break;
            }
            case ExportMiscellaneousUtils.EXPORT_LOG_PATH: {
                File file = new File(settingsValue);
                if (file.isDirectory()) {
                    String path = file.getPath();
                    if (!path.endsWith(File.separator)) {
                        path += File.separator;
                    }
                    ExportMiscellaneousUtils.setLogPath(path);
                }
                break;
            }
            case ExportMiscellaneousUtils.EXPORT_PROPERTIES_FILE_PATH: {
                File file = new File(settingsValue);
                File[] propertyFiles = file.listFiles((dir, name) -> name.toLowerCase().endsWith(".properties"));
                if (propertyFiles != null && propertyFiles.length > 0) {
                    String path = file.getPath();
                    if (!path.endsWith(File.separator)) {
                        path += File.separator;
                    }
                    ExportMiscellaneousUtils.setPropertiesPath(projectPropertiesPath = path);
                } else {
                    throw new ExportStagingException("Invalid Properties Path provided as startup argument");
                }
                break;
            }
            default: {
                throw new ExportStagingException("Invalid Option " + settingsKey + ". Check ExportStaging Documentation for valid syntax");
            }
        }
    }


    private static boolean loadApplicationContext() {
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("spring/profileLoader.xml");
        properties = (Properties) applicationContext.getBean("exportProperties");
        ExportInitializerUtils exportInitializerUtils = (ExportInitializerUtils) applicationContext.getBean("exportInitializerUtils");
        List<String> activeProfiles = new ArrayList<>(exportInitializerUtils.createProfiles(
                Boolean.parseBoolean(properties.getProperty("export.enable.master.subscriber")),
                Boolean.parseBoolean(properties.getProperty("export.enable.elastic.subscriber")),
                Boolean.parseBoolean(properties.getProperty("export.enable.websocket.subscriber")),
                Boolean.parseBoolean(properties.getProperty("export.enable.master.producer"))
        ));
        try {
            ConfigurableEnvironment environment = applicationContext.getEnvironment();
            environment.setActiveProfiles(activeProfiles.toArray(new String[0]));
            applicationContext.refresh();
            applicationContext.registerShutdownHook();

            exportActiveMQUtils = (ExportActiveMQUtils) applicationContext.getBean("exportActiveMQUtils");
            System.out.println("\n----------------------Export Staging Status-------------------------");
            System.out.println("Export Staging Online: " + new Date());
            exportDbName = properties.getProperty("export.database.name");
            exportActiveMQUtils.printQueueSubscriberInfo(exportDbName);
        } catch (Exception e) {
            logger.error("Exception while loading application context:" + e.getMessage());
            if (e.getCause() instanceof CannotCreateConnectionException) {
                System.out.println(e.getCause().getMessage());
                System.exit(1);
            }
            System.err.println(e.getMessage());
            printSubscriberInfo = false;
            return false;
        }
        return true;
    }


    private static void configureProjectPathProperty(String[] startUpSettings) {
        if (projectName == null) {
            getProjectNameFromProgramArgs(startUpSettings);
        }
        ExportMiscellaneousUtils.isProjectNameLoaded(projectName);
        if (projectPropertiesPath == null) {
            projectPropertiesPath = ExportMiscellaneousUtils.getProjectPropertiesPath(projectName);
        }
        if (projectPropertiesPath != null) {
            System.setProperty("project.path", projectPropertiesPath);
            configureExportLogging();
        } else {
            String msg = "Error while determining project path. Please refer Product documentation to determine correct settings.";
            System.out.println(msg);
            logger.error(msg);
            System.out.println("System Exiting now...");
            System.exit(0);
        }

    }


    private static void getProjectNameFromProgramArgs(String[] startUpSettings) {
        ArrayList<String> projectNames = new ArrayList<String>();
        if (startUpSettings.length > 0) {
            projectName = startUpSettings[0];
        } else {
            try {
                projectNames = ExportMiscellaneousUtils.getProjectNames();
            } catch (ExportStagingException e) {
                System.out.println("Exception while getting project names. Messages:" + e.getMessage());
            }
            if (projectNames.size() == 1) {
                projectName = projectNames.get(0);
            } else {
                System.out.print("Please provide project name: ");
                projectName = new Scanner(System.in).next();
            }
        }
    }


    private static void getExportExecutorProcessId() {
        String exportExecutorProcessId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        String filePath = null;
        if (projectPropertiesPath != null) {
            File confDir = new File(projectPropertiesPath);
            filePath = confDir.getParent();
            filePath += File.separator + "ExportExecutor.pid";
            try {
                if (filePath != null) {
                    File file = new File(filePath);
                    file.deleteOnExit();
                    FileUtils.writeStringToFile(file, exportExecutorProcessId);
                }
            } catch (Exception e) {
                logger.error("Error while writing ProcessID. File Path: " + filePath + " Error Message:" + e.getMessage());
            }
        }
    }


    /**
     * Method will check the startup parameter contains elastic index update param or not
     *
     * @param startUpSettings startup parameters
     * @return true if startup param contains elastic index creation param otherwise false
     */
    private static boolean isElasticIndexUpdater(String[] startUpSettings) {
        List<String> param = Arrays.asList(startUpSettings);

        return param.contains(ExportMiscellaneousUtils.EXPORT_MODE + ":" + ExportMiscellaneousUtils.EXPORT_ELASTIC_INDEX_UPDATER);
    }


    /**
     * Method will load application context only regarding elastic search index creation
     * No Need to load all profiles and configuration
     * Required configuration are specified in the modelupdater.xml
     *
     * @param startUpSettings array of string containing information of argument provided by runtime
     */
    private static void loadElasticSpecificContext(String[] startUpSettings) {
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("spring/modelupdater.xml");
        ElasticIndexUpdater indexUpdater = applicationContext.getBean("elasticIndexUpdater", ElasticIndexUpdater.class);
        indexUpdater.updateIndex(startUpSettings);

    }

    /**
     * Method will set system parameter for export log folder and debug mode.
     */
    private static void configureExportLogging() {
        String projectLogPath = ExportMiscellaneousUtils.getLogFolderPath(projectName);
        System.setProperty("exportLoggingPath", projectLogPath + "Export" + File.separator);

        if ("1".equals(ExportMiscellaneousUtils.getPropertyValue("export.logger.debug.mode", ExportMiscellaneousUtils.CORE_PROPERTIES, projectName))) {
            System.setProperty("exportDebugLoggerMode", "DEBUG");
        } else {
            System.setProperty("exportDebugLoggerMode", "OFF");
        }
        logger = LogManager.getLogger("exportstaging");
    }
}