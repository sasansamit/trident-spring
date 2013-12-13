package com.kaseya.trident.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.kaseya.trident.Utils;

@SuppressWarnings("serial")
public class JSONAdapterFunction extends BaseFunction {

    protected static Logger logger = Logger
            .getLogger(JSONAdapterFunction.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        JSONObject jsonObj = (JSONObject) JSONValue.parse(tuple.getString(0));

        logger.info("Kafka Input: " + tuple.getString(0));

        List<Object> fields = new ArrayList<Object>();
        fields.add(jsonObj.get(Utils.kTimeStamp));
        fields.add(jsonObj.get(Utils.kDeviceId));
        fields.add(Double.parseDouble(jsonObj.get(Utils.kMemory).toString()));

        collector.emit(fields);
    }

}
