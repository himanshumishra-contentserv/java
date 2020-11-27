package com.exportstaging.subscribers;

import org.json.simple.JSONArray;

import com.exportstaging.api.exception.CannotCreateConnectionException;

/**
 * The Subscriber interface is used to create and receive messages via custom subscriber.
 * Having two abstract methods <br>
 * <b>receiveMessage() : </b> used for receiving message from ActiveMQ register topic.<br>
 * <b>startSubscriber() : </b> used for create consumer
 * <b>stopSubscriber() : </b> used for stop the consumer
 */
public interface Subscriber {

    void startSubscriber() throws CannotCreateConnectionException;
    
    /**
     * Removes dynamic attributes from item message</br>
     * override it for a subscriber if need to change the behavior
     * 
     * @param array
     * @param itemType
     */
    void removeDynamicAttributes(JSONArray array, String itemType);
}
