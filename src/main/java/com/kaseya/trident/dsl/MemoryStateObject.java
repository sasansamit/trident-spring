package com.kaseya.trident.dsl;

import java.util.HashMap;
import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

public class MemoryStateObject implements State {

    @SuppressWarnings("serial")
    public static class Factory implements StateFactory {
        @Override
        public State makeState(@SuppressWarnings("rawtypes") Map conf,
                IMetricsContext metrics,
                int partitionIndex,
                int numPartitions) {
            return new MemoryStateObject();
        }
    }

    private HashMap<String, Object> _store;

    public MemoryStateObject() {
        this._store = new HashMap<String, Object>();
    }

    public void beginCommit(Long txid) {
    }

    public void commit(Long txid) {
    }

    public boolean storeData(String key, Object value) {
        if (key == null || value == null) {
            System.out.println("Invalid Input");
            return false;
        }
        _store.put(key, value);
        return true;
    }

    public Object getData(String key) {
        return _store.get(key);
    }

    public String toString() {
        return "sample data store containing a map: " + _store;
    }
}
