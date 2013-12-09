package com.kaseya.trident;

import java.util.List;

import org.springframework.beans.factory.annotation.Required;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.spout.IBatchSpout;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.spout.ITridentSpout;

import com.kaseya.trident.operations.IOperation;

public class StreamWrapper {
    protected String _spoutNodeName;
    protected TridentTopology _topology;
    protected List<IOperation> _operations;
    protected IStreamFactory _streamFactory;
    protected Stream _stream;

    @Required
    public void setSpoutNodeName(String spoutNodeName) {
        this._spoutNodeName = spoutNodeName;
    }

    @Required
    public void setTopology(TridentTopology topology) {
        this._topology = topology;
    }

    @Required
    public void setOperations(List<IOperation> operations) {
        this._operations = operations;
    }

    public void setBatchSpout(final IBatchSpout spout) {
        _streamFactory = new BatchSpoutFactory(spout);
    }

    public void setTridentSpout(final ITridentSpout<?> spout) {
        _streamFactory = new TridentSpoutFactory(spout);
    }

    public void setPartitionedTridentSpout(final IPartitionedTridentSpout<?> spout) {
        _streamFactory = new PartitionedTridentSpoutFactory(spout);
    }

    public void setOpaquePartitionedTridentSpout(final IOpaquePartitionedTridentSpout<?> spout) {
        _streamFactory = new OpaquePartitionedTridentSpoutFactory(spout);
    }

    public Stream getStream() {
        return _stream;
    }

    public void build() {
        if (_streamFactory == null) {
            throw new RuntimeException("Trying to build a Stream without a spout. StreamWrapper needs to be set with a spout.");
        }

        Object streamObj = _streamFactory.getStream();

        if (_operations != null) {
            for (IOperation op : _operations) {
                streamObj = op.addStreamProcessor(streamObj);
            }
        }

        _stream = (Stream) streamObj;
    }

    private interface IStreamFactory {
        Stream getStream();
    }

    private class BatchSpoutFactory implements IStreamFactory {

        private IBatchSpout _spout;

        public BatchSpoutFactory(IBatchSpout spout) {
            this._spout = spout;
        }

        public Stream getStream() {
            return _topology.newStream(_spoutNodeName, _spout);
        }
    }

    private class TridentSpoutFactory implements IStreamFactory {

        private ITridentSpout<?> _spout;

        public TridentSpoutFactory(ITridentSpout<?> spout) {
            this._spout = spout;
        }

        public Stream getStream() {
            return _topology.newStream(_spoutNodeName, _spout);
        }
    }

    private class PartitionedTridentSpoutFactory implements IStreamFactory {

        private IPartitionedTridentSpout<?> _spout;

        public PartitionedTridentSpoutFactory(IPartitionedTridentSpout<?> spout) {
            this._spout = spout;
        }

        public Stream getStream() {
            return _topology.newStream(_spoutNodeName, _spout);
        }
    }

    private class OpaquePartitionedTridentSpoutFactory implements
            IStreamFactory {

        private IOpaquePartitionedTridentSpout<?> _spout;

        public OpaquePartitionedTridentSpoutFactory(IOpaquePartitionedTridentSpout<?> spout) {
            this._spout = spout;
        }

        public Stream getStream() {
            return _topology.newStream(_spoutNodeName, _spout);
        }
    }
}
//
// public class StreamFactory implements FactoryBean<Object> {
//
// protected String _spoutNodeName;
// protected IBatchSpout _spout;
// protected TridentTopology _topology;
// protected List<IOperation> _operations;
//
// public StreamFactory(final IBatchSpout spout,
// final TridentTopology topology) {
// this._spout = spout;
// this._topology = topology;
// }
//
// public void setSpoutNodeName(final String name) {
// this._spoutNodeName = name;
// }
//
// public void setSpout(final IBatchSpout spout) {
// this._spout = spout;
// }
//
// public void setOperations(List<IOperation> operations) {
// this._operations = operations;
// }
//
// public Object getObject() throws Exception {
// Object stream = _topology
// .newStream(_spoutNodeName, _spout);
//
// if (_operations != null) {
// for (IOperation op : _operations) {
// stream = op.visit(stream);
// }
// }
//
// return stream;
// }
//
// public Class<?> getObjectType() {
// return Object.class;
// }
//
// public boolean isSingleton() {
// return false;
// }
//
// }
