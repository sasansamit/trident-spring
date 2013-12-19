package com.kaseya.trident.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

@SuppressWarnings("serial")
public class JSONAdapterFunction extends BaseFunction {

    protected static final Logger logger = Logger
            .getLogger(JSONAdapterFunction.class);
    final private List<String> _tuple;
    
    public JSONAdapterFunction(final List<String> tuple) {
        this._tuple = tuple;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        JSONObject jsonObj = (JSONObject) JSONValue.parse(tuple.getString(0));

        logger.info("Kafka Input: " + tuple.getString(0));

        List<Object> fields = new ArrayList<Object>();
        
        for (String element : _tuple) {
            fields.add(jsonObj.get(element));
        }

        collector.emit(fields);
    }

}
