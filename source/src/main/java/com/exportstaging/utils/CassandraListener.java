package com.exportstaging.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component("stateListener")
public class CassandraListener implements Host.StateListener {
    @Value("${activemq.subscriber.master}")
    private String masterSubscriber;

    private Map<InetSocketAddress, Host> hostMap = new ConcurrentHashMap<>();
    private volatile boolean hostAvailability = false;
    private final static Logger logger = LogManager.getLogger("exportstaging");

    public boolean isHostAvailable() {
        return hostAvailability;
    }

    private void setHostAvailability(boolean hostAvailability) {
        this.hostAvailability = hostAvailability;
    }

    @Override
    public void onAdd(Host host) {
        addHost(host);
    }

    @Override
    public void onUp(Host host) {
        addHost(host);
        String message = host.getSocketAddress() + " is back online. Connected to " + hostMap.keySet();
        logger.info(message);
    }

    private void addHost(Host host) {
        hostMap.put(host.getSocketAddress(), host);
        setHostAvailability(true);
    }

    @Override
    public void onDown(Host host) {
        logger.info("[ " + masterSubscriber + " ] Node " + host.getSocketAddress() + " went down. Still connected to " + hostMap.keySet());
        removeHost(host);
    }

    @Override
    public void onRemove(Host host) {
        String msg = host.getSocketAddress() + " removed from cluster";
        logger.info("[" + masterSubscriber + "] " + msg);
        removeHost(host);
    }

    private void removeHost(Host host) {
        hostMap.remove(host.getSocketAddress());
        if (hostMap.isEmpty()) {
            setHostAvailability(false);
        }
    }

    @Override
    public void onRegister(Cluster cluster) {
    }

    @Override
    public void onUnregister(Cluster cluster) {
    }
}
