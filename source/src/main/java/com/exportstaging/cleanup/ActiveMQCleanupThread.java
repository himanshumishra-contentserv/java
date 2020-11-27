package com.exportstaging.cleanup;

import com.exportstaging.connectors.messagingqueue.ActiveMQMBeanConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;

@Scope("prototype")
@Component("activeMQCleanupThread")
public class ActiveMQCleanupThread extends Thread {

    private String queueName;
    private List<String> itemTypes;

    @Autowired
    private ActiveMQMBeanConnection mBeanConnection;

    private final static Logger logger = LogManager.getLogger("exportstaging");

    @Override
    public void run() {
        String message = "Performing cleanup of messages on " + queueName + " for item types " + itemTypes.toString();
        logger.info(message);
        System.out.println(message);
        try {
            mBeanConnection.removeMessagesUsingSelector(queueName, itemTypes);
        } catch (Exception e) {
            logger.error("[Subscriber]: Exception while performing cleanup on queue " + queueName +
                    " for item types: " + itemTypes.toString() + ". Error Cause: " + e.getMessage());
        }
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setItemTypes(List<String> itemTypes) {
        this.itemTypes = itemTypes;
    }
}
