package com.kaseya.trident;

import java.util.List;

import org.springframework.beans.factory.FactoryBean;

import storm.trident.TridentTopology;
import storm.trident.spout.IBatchSpout;

import com.kaseya.trident.operations.IOperation;

public class StreamFactory implements FactoryBean<Object> {

    protected String _spoutNodeName;
    protected IBatchSpout _spout;
    protected TridentTopology _topology;
    protected List<IOperation> _operations;

    public StreamFactory(final IBatchSpout spout,
                         final TridentTopology topology) {
        this._spout = spout;
        this._topology = topology;
    }

    public void setSpoutNodeName(final String name) {
        this._spoutNodeName = name;
    }

    public void setSpout(final IBatchSpout spout) {
        this._spout = spout;
    }

    public void setOperations(List<IOperation> operations) {
        this._operations = operations;
    }

    public Object getObject() throws Exception {
        Object stream = _topology
                .newStream(_spoutNodeName, _spout);

        if (_operations != null) {
            for (IOperation op : _operations) {
                stream = op.visit(stream);
            }
        }

        return stream;
    }

    public Class<?> getObjectType() {
        return Object.class;
    }

    public boolean isSingleton() {
        return false;
    }

}
