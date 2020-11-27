package com.exportstaging.initial;

import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

@Component("exportInitializerUtils")
public class ExportInitializerUtils {

    @Value("${activemq.connection.client}")
    private String connectionClient;

    @Value("${activemq.url.host}")
    private String activeMQConnectionHost;

    @Value("${cassandra.connection.url}")
    private String cassandraConnectionHost;

    @Value("${elasticsearch.address}")
    private String elasticConnectionHost;

    @Value("${jdbc.address}")
    private String mySqlConnectionHost;

    @Value("${core.project.name}")
    private String projectName;

    private static final String CONST_MASTER_PROFILE = "MasterProfile";
    private static final String CONST_ELASTIC_PROFILE = "ElasticProfile";
    private static final String CONST_WEBSOCKET_PROFILE = "WebSocketProfile";

    private static final String CONST_BROKER_PROFILE = "BrokerProfile";
    private static final String CONST_MASTER_SUBSCRIBER_PROFILE = "MasterSubscriberProfile";
    private static final String CONST_ELASTIC_SUBSCRIBER_PROFILE = "ElasticSubscriberProfile";
    private static final String CONST_WEBSOCKET_SUBSCRIBER_PROFILE = "WebSocketSubscriberProfile";
    private static final String CONST_MASTER_PRODUCER_PROFILE = "MasterProducerProfile";

    /**
     * This function creates a socket connection to ActiveMQ host address and port number and checks if the host and
     * port are already occupied. If occupied, it returns false, else true.
     *
     * @return boolean
     */
    public boolean isActiveMQRunning() {
        String host = activeMQConnectionHost;
        int port = ExportMiscellaneousUtils.DEFAULT_PORT_ACTIVEMQ;
        return getInstanceStatus(host, port);
    }

    public boolean isCassandraRunning() {
        String host = cassandraConnectionHost;
        int port = 0;
        return getInstanceStatus(host, port);
    }

    public boolean isElasticSearchRunning() {
        String host = elasticConnectionHost;
        int port = ExportMiscellaneousUtils.DEFAULT_TCP_PORT_ELASTICSEARCH;
        return getInstanceStatus(host, port);
    }

    public boolean isMySQLRunning() {
        String host = mySqlConnectionHost;
        int port = ExportMiscellaneousUtils.DEFAULT_PORT_MYSQL;
        return getInstanceStatus(host, port);
    }

    private boolean getInstanceStatus(String host, int port) {
        Socket socket = null;
        try {
            socket = new Socket(host, port);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.out.println("Unable to close socket connection");
                }
            }
        }
    }

    /**
     * This function creates the List of Profiles to be activated during ExportExecutor initialization.
     *
     * @param profiles Commandline arguments for ExportExecutor Profile.
     * @return List of Export components that would be activated for a specific profile.
     * @throws ExportStagingException when invalid ExportExecutor Profile is provided.
     */
    public List<String> createProfiles(String[] profiles) throws ExportStagingException {
        List<String> activeProfiles = new ArrayList<>();
        activeProfiles.add(CONST_BROKER_PROFILE);
        switch (profiles[1]) {
            case CONST_MASTER_PROFILE:
                activeProfiles.add(CONST_MASTER_SUBSCRIBER_PROFILE);
                break;
            case CONST_ELASTIC_PROFILE:
                activeProfiles.add(CONST_ELASTIC_SUBSCRIBER_PROFILE);
                break;
            case CONST_WEBSOCKET_PROFILE:
                activeProfiles.add(CONST_WEBSOCKET_SUBSCRIBER_PROFILE);
                break;
            default:
                throw new ExportStagingException("\nIllegal Profile Exception. Check the profile options provided" +
                        " while " +
                        "running the jar.\nAvailable Profiles:\n" +
                        "1. " + CONST_MASTER_PROFILE + "\n" +
                        "2. " + CONST_ELASTIC_PROFILE + "\n" +
                		"3. " + CONST_WEBSOCKET_PROFILE + "\n");
        }
        activeProfiles.add(CONST_MASTER_PRODUCER_PROFILE);

        return activeProfiles;
    }

    /**
     * This function creates the List of Profiles to be activated during ExportExecutor initialization. This is
     * overloaded function which reads the core properties file to determine which Export Components need to be
     * initialized.
     *
     * @param masterSubscriberFlag  Start MasterSubscriber
     * @param elasticSubscriberFlag Start ElasticSubscriber
     * @param masterProducerFlag    Start MasterProducer
     * @return List of Export components that would be activated.
     */
    public List<String> createProfiles(boolean masterSubscriberFlag, boolean
            elasticSubscriberFlag, boolean websocketSubscriberFlag, boolean masterProducerFlag) {
        List<String> activeProfiles = new ArrayList<>();
        activeProfiles.add(CONST_BROKER_PROFILE);

        if (masterSubscriberFlag) {
            activeProfiles.add(CONST_MASTER_SUBSCRIBER_PROFILE);
        }
        if (elasticSubscriberFlag) {
            activeProfiles.add(CONST_ELASTIC_SUBSCRIBER_PROFILE);
        }
        if (websocketSubscriberFlag) {
            activeProfiles.add(CONST_WEBSOCKET_SUBSCRIBER_PROFILE);
        }
        if (masterProducerFlag) {
            activeProfiles.add(CONST_MASTER_PRODUCER_PROFILE);
        }
        return activeProfiles;
    }


    /**
     * Checks if Project name is set inside core.properties.
     *
     * @return false if project name is set.
     */
    public boolean isDefaultProjectName() {
        return !ExportMiscellaneousUtils.getPropertyValue("core.project.name", ExportMiscellaneousUtils.CORE_PROPERTIES, projectName).equals(projectName);
    }
}
