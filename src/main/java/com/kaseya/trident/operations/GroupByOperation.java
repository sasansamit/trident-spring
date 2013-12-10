package com.kaseya.trident.operations;

import java.util.List;

import storm.trident.Stream;
import backtype.storm.tuple.Fields;

import com.kaseya.trident.Utils;

public class GroupByOperation implements IStreamOperation {

    protected final Fields _groupBy;

    public GroupByOperation(final List<String> groupBy) {
        this._groupBy = new Fields(groupBy);
    }

    public Object addStreamProcessor(Object stream) {

        Utils.ValidateOperationType(this, stream, Stream.class);

        Stream castStream = (Stream) stream;
        return castStream.groupBy(_groupBy);
    }
}
