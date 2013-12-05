package com.kaseya.trident;

import java.util.List;

import backtype.storm.Config;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.fluent.IAggregatableStream;

public class SingleTopologySubmission {

    protected final String _topologyId;
    protected final TridentTopology _topology;
    protected final Config _config;
    protected List<IAggregatableStream> _streams;

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

    public TridentTopology getTopology() {
        return _topology;
    }

    public Config getConfig() {
        return _config;
    }

    public List<IAggregatableStream> getStreams() {
        return _streams;
    }

    public void setStreams(List<IAggregatableStream> streams) {
        this._streams = streams;
    }
}
