package com.exportstaging.producers;

import org.springframework.scheduling.TaskScheduler;

import javax.jms.MapMessage;

public interface Producer {
    //This producer ID should be 2^producer_number
    int masterProducerID = 2;
    int projectProducerID = 4;

    boolean sendMessage(MapMessage message, int JMSPriority);
    void startProducer();
    void stopProducer();
    MapMessage getMapMessage();
    TaskScheduler getTaskScheduler();
    int getProducerID();
    String getProducerName();
}
