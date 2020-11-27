package com.exportstaging.producers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;

public class ProjectProducer extends MasterProducer {

    @Autowired
    private TaskScheduler projectProducerScheduler;

    @Override
    public void startProducer() {
        producerName = this.getClass().getSimpleName();
        String msgInfo = "[" + producerName + "] : initializing...";
        System.out.println(msgInfo);
        logger.info(msgInfo);
        String topicName = activeMQSpringConnection.getTopicName(exportDatabaseName, true);
        messageProducer = activeMQSpringConnection.getProducer(topicName, producerName, getProducerSession());
    }

    @Override
    public TaskScheduler getTaskScheduler() {
        return projectProducerScheduler;
    }

    @Override
    public String getProducerName() {
        return producerName;
    }

    @Override
    public int getProducerID() {
        return projectProducerID;
    }
}