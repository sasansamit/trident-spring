package com.kaseya.trident.operations;

import java.util.List;

import storm.trident.Stream;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.CombinerAggregator;
import storm.trident.state.StateFactory;
import backtype.storm.tuple.Fields;

import com.kaseya.trident.Utils;

public class PersistentAggregateOperation implements IStreamOperation {

    final protected Fields _outputTuples;
    final protected CombinerAggregator<?> _aggregate;
    final protected StateFactory _stateFactory;

    public PersistentAggregateOperation(final StateFactory stateFactory,
                                        final CombinerAggregator<?> aggregate,
                                        final List<String> outputTuples) {
        this._stateFactory = stateFactory;
        this._aggregate = aggregate;
        this._outputTuples = new Fields(outputTuples);
    }

    public Object addStreamProcessor(Object stream) {
        Utils.ValidateOperationType(this, stream, GroupedStream.class);

        Stream castStream = (Stream) stream;
        return castStream.persistentAggregate(_stateFactory,
                                              _aggregate,
                                              _outputTuples);
    }
}
