package com.kaseya.trident.operations;

import java.util.List;

import storm.trident.Stream;
import storm.trident.operation.Filter;
import storm.trident.operation.Function;
import backtype.storm.tuple.Fields;

import com.kaseya.trident.Utils;

public class EachOperation implements IOperation {
    protected final BaseEachHelper _eachHelper;

    public EachOperation(final List<String> inputTuples,
                         final Function function,
                         final List<String> outputTuples) {
        _eachHelper = new FunctionEachHelper(inputTuples,
                                             function,
                                             outputTuples);
    }

    public EachOperation(final List<String> inputTuples, final Filter filter) {
        _eachHelper = new FilterEachHelper(inputTuples, filter);
    }

    public Object addStreamProcessor(Object stream) {

        Utils.ValidateOperationType(this, stream, Stream.class);

        return _eachHelper.visit((Stream) stream);
    }

    private abstract class BaseEachHelper {
        protected Fields _inputTuples;

        public BaseEachHelper(final List<String> inputTuples) {
            this._inputTuples = new Fields(inputTuples);
        }

        public abstract Stream visit(Stream stream);
    }

    private class FunctionEachHelper extends BaseEachHelper {
        protected Function _function;
        protected Fields _outputTuples;

        public FunctionEachHelper(final List<String> inputTuples,
                                  final Function function,
                                  final List<String> outputTuples) {
            super(inputTuples);
            _function = function;
            _outputTuples = new Fields(outputTuples);
        }

        @Override
        public Stream visit(Stream stream) {
            return stream.each(_inputTuples, _function, _outputTuples);
        }
    }

    private class FilterEachHelper extends BaseEachHelper {

        protected Filter _filter;

        public FilterEachHelper(final List<String> inputTuples,
                                final Filter filter) {
            super(inputTuples);
            _filter = filter;
        }

        @Override
        public Stream visit(Stream stream) {
            return stream.each(_inputTuples, _filter);
        }
    }
}
