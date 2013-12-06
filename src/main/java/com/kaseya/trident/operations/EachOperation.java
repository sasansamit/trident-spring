package com.kaseya.trident.operations;

import java.util.List;

import storm.trident.Stream;
import storm.trident.operation.Function;
import backtype.storm.tuple.Fields;

import com.kaseya.trident.Utils;

public class EachOperation implements IOperation {
    protected final Fields _input;
    protected final Fields _output;
    protected final Function _function;

    public EachOperation(final List<String> inputTuples,
                         final Function function,
                         final List<String> outputTuples) {
        this._input = new Fields(inputTuples);
        this._function = function;
        this._output = new Fields(outputTuples);
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

        Utils.ValidateOperationType(this, stream, Stream.class);

        Stream castStream = (Stream) stream;
        return castStream.each(_input, _function, _output);
    }
}
