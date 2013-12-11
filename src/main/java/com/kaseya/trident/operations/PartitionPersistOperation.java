package com.kaseya.trident.operations;

import java.util.List;

import storm.trident.Stream;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import backtype.storm.tuple.Fields;

import com.kaseya.trident.Utils;

public class PartitionPersistOperation implements IStreamOperation {

    protected final StateFactory _stateFactory;
    protected final Fields _inputFields;
    protected final StateUpdater<?> _updater;

    public PartitionPersistOperation(final StateFactory stateFactory,
                                     final List<String> inputFields,
                                     final StateUpdater<?> updater) {
        this._stateFactory = stateFactory;
        this._inputFields = new Fields(inputFields);
        this._updater = updater;
    }

    public Object addStreamProcessor(Object stream) {
        Utils.ValidateOperationType(this, stream, Stream.class);

        Stream castStream = (Stream) stream;
        return castStream.partitionPersist(_stateFactory,
                                           _inputFields,
                                           _updater);
    }
}
