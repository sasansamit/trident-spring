package com.kaseya.trident;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

public interface ITopologySubmission {

    /**
     * Getter for Topology Id
     * 
     * @return Topology Id
     */
    public String getTopologyId();

    /**
     * Getter for Trident Config
     * 
     * @return Trident config
     */
    public Config getConfig();

    /**
     * Getter for Storm Topology
     * 
     * @return Storm Topology
     */
    public StormTopology getTopology();

}
