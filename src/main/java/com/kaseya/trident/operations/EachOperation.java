package com.kaseya.trident.operations;

import storm.trident.Stream;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.Function;
import backtype.storm.tuple.Fields;

public class EachOperation implements IOperation {
    protected final Fields _input;
    protected final Fields _output;
    protected final Function _function;

    public EachOperation(final Fields input,
                         final Function function,
                         final Fields output) {
        this._input = input;
        this._function = function;
        this._output = output;
    }

    public Fields getInput() {
        return _input;
    }

    public Fields getOutput() {
        return _output;
    }

    public Function getFunction() {
        return _function;
    }

    public Object visit(Object stream) {
        if (!(stream instanceof Stream)) {
            throw new RuntimeException("EachOperation can only be called on a Strean. Current stream is of type [" + stream.getClass() + "]");
        }
        Stream castStream = (Stream) stream;
        return castStream.each(_input, _function, _output);
    }
}
