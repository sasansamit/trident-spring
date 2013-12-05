package com.kaseya.trident.operations;

import storm.trident.Stream;
import storm.trident.fluent.IAggregatableStream;
import backtype.storm.tuple.Fields;

public class GroupByOperation implements IOperation {

    protected final Fields _groupBy;

    public GroupByOperation(final Fields groupBy) {
        this._groupBy = groupBy;
    }

    public Object visit(Object stream) {
        if (!(stream instanceof Stream)) {
            throw new RuntimeException("GroupByOperation can only be called on a Stream. Current stream is of type [" + stream.getClass());
        }
        Stream castStream = (Stream)stream;
        return castStream.groupBy(_groupBy);
    }
}
