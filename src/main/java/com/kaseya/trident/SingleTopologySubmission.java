package com.kaseya.trident;

import java.util.List;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

public class SingleTopologySubmission implements ITopologySubmission {

    protected final String _topologyId;
    protected final TridentTopology _topology;
    protected final Config _config;
    protected List<StreamWrapper> _streams;

    public SingleTopologySubmission(final String topologyId,
                                    final TridentTopology topology,
                                    final Config config) {
        this._topologyId = topologyId;
        this._topology = topology;
        this._config = config;
    }

    // Getters & Setter
    public String getTopologyId() {
        return _topologyId;
    }

    public StormTopology getTopology() {
        return _topology.build();
    }

    public Config getConfig() {
        return _config;
    }

    public List<StreamWrapper> getStreams() {
        return _streams;
    }

    public void setStreams(List<StreamWrapper> streams) {
        this._streams = streams;
    }
}
