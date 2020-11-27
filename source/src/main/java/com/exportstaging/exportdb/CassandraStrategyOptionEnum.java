package com.exportstaging.exportdb;

public enum CassandraStrategyOptionEnum {

    SIMPLE_STRATEGY("SimpleStrategy"),
    NETWORK_TOPOLOGY_STRATEGY("NetworkTopologyStrategy");

    public final String strategy;

    CassandraStrategyOptionEnum(String strategy) {
        this.strategy = strategy;
    }

    public String getStrategy() {
        return strategy;
    }
}
