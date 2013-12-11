package com.kaseya.trident.operations;

import java.util.List;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.state.QueryFunction;
import backtype.storm.tuple.Fields;

import com.kaseya.trident.Utils;

public class StateQueryOperation implements IStreamOperation {
    final protected TridentState _state;
    final protected QueryFunction<?, ?> _queryFunction;
    final protected Fields _outputTuples;

    public StateQueryOperation(final TridentState state,
                               final QueryFunction<?, ?> queryFunction,
                               final List<String> outputTuples) {
        this._state = state;
        this._queryFunction = queryFunction;
        this._outputTuples = new Fields(outputTuples);

    }

    public Object addStreamProcessor(Object stream) {
        Utils.ValidateOperationType(this, stream, Stream.class);
        
        Stream castStream = (Stream) stream;
        return castStream.stateQuery(_state, _queryFunction, _outputTuples);
    }
}
