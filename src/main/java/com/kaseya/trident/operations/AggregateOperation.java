package com.kaseya.trident.operations;

import storm.trident.fluent.GroupedStream;
import storm.trident.operation.CombinerAggregator;
import backtype.storm.tuple.Fields;

public class AggregateOperation implements IOperation {

    protected final Fields _input;
    protected final CombinerAggregator<?> _aggregator;
    protected final Fields _output;
    
    public AggregateOperation(final Fields input, final CombinerAggregator<?> aggregator, final Fields output) {
        this._input = input;
        this._aggregator = aggregator;
        this._output = output;
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

    public Object visit(Object stream) {
        if (!(stream instanceof GroupedStream)) {
            throw new RuntimeException("AggregateOperation can only be called on a GroupedStream. Current stream is of type [" + stream.getClass() + "]");
        }
        GroupedStream castStream = (GroupedStream) stream;
        return castStream.aggregate(_input, _aggregator, _output);
    }

}
