package com.exportstaging.subscribers;

import com.exportstaging.api.exception.CannotCreateConnectionException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.util.Scanner;

/**
 * The AbstractCustomSubscriber class is used to create and receive messages via custom subscriber.
 */
public abstract class AbstractCustomSubscriber extends AbstractSubscriber {
    private static String projectName;

    final public void startSubscriber() {
        if (projectName == null) {
            System.out.print("Provide project name: ");
            Scanner scanner = new Scanner(System.in);
            setProjectName(scanner.nextLine());
        }
        ExportMiscellaneousUtils.isProjectNameLoaded(projectName);
        System.setProperty("project.path", ExportMiscellaneousUtils.getProjectPropertiesPath(projectName));
        configureExportLogging();
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("spring/custombroker.xml");
        context.registerShutdownHook();
        context.getAutowireCapableBeanFactory().autowireBean(this);

        try {
            super.startSubscriber();
        } catch (CannotCreateConnectionException ignored) {
        }
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
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    @Override
    final protected boolean isCustomSubscriber() {
        return true;
    }
}
