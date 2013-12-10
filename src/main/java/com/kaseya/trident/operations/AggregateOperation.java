package com.kaseya.trident.operations;

import java.util.List;

import storm.trident.fluent.GroupedStream;
import storm.trident.operation.CombinerAggregator;
import backtype.storm.tuple.Fields;

import com.kaseya.trident.Utils;

public class AggregateOperation implements IStreamOperation {

    protected final Fields _input;
    protected final CombinerAggregator<?> _aggregator;
    protected final Fields _output;

    public AggregateOperation(final List<String> inputTuples,
                              final CombinerAggregator<?> aggregator,
                              final List<String> outputTuples) {
        this._input = new Fields(inputTuples);
        this._aggregator = aggregator;
        this._output = new Fields(outputTuples);
    }

    public Fields getInput() {
        return _input;
    }

    public CombinerAggregator<?> getAggregator() {
        return _aggregator;
    }

    public Fields getOutput() {
        return _output;
    }

    public Object addStreamProcessor(Object stream) {

        Utils.ValidateOperationType(this, stream, GroupedStream.class);

        GroupedStream castStream = (GroupedStream) stream;
        return castStream.aggregate(_input, _aggregator, _output);
    }

}
