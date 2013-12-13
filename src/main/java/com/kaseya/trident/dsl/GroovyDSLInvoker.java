package com.kaseya.trident.dsl;

import groovy.lang.Binding;
import groovy.util.GroovyScriptEngine;
import groovy.util.ResourceException;
import groovy.util.ScriptException;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import com.kaseya.trident.Utils;

@SuppressWarnings("serial")
public class GroovyDSLInvoker implements StateUpdater<MemoryStateObject> {
    static protected Logger sLogger = Logger.getLogger(GroovyDSLInvoker.class);

    private transient GroovyScriptEngine _scriptEngine;
    private transient Binding _binding;
    // private transient CompilerConfiguration _config;
    // private transient SecureASTCustomizer _security;
    private File _file;
    private transient HashMap<String, Object> _input;
    private transient MemoryStateObject _state;

    public GroovyDSLInvoker(final String filePath) throws IOException {
        this._file = new File(filePath);
        init_();
    }

    protected void init_() throws IOException {
        this._scriptEngine = new GroovyScriptEngine(_file.getParent());
        this._binding = new Binding();
        // this._config = new CompilerConfiguration();
        // this._security = new SecureASTCustomizer();
        this._input = new HashMap<String, Object>();
        this._state = new MemoryStateObject();

        // List<String> importsWhiteList = new ArrayList<String>();
        // importsWhiteList.add("java.lang.Math");
        // _security.setStarImportsWhitelist(importsWhiteList);
        // _config.addCompilationCustomizers(_security);
        // _scriptEngine.setConfig(_config);

        _binding.setVariable("input", _input);
        _binding.setVariable("dataStore", _state);
    }

    public void executeScript_() throws ResourceException, ScriptException {
        _scriptEngine.run(_file.getAbsolutePath(), _binding);
    }

    public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
    }

    public void cleanup() {
    }

    public void updateState(MemoryStateObject state, List<TridentTuple> tuples,
            TridentCollector collector) {

        for (TridentTuple tuple : tuples) {
            _input.clear();
            _input.put(Utils.kMemory, tuple.getValueByField(Utils.kMemory));
            _input.put(Utils.kDeviceId, tuple.getStringByField(Utils.kDeviceId));
            _input.put(Utils.kTimeStamp,
                       tuple.getValueByField(Utils.kTimeStamp));

            try {
                executeScript_();
            } catch (ResourceException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ScriptException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    // @Override
    // public void updateState(MemoryMapState<?> state, List<TridentTuple>
    // tuples,
    // TridentCollector collector) {
    // for (TridentTuple tuple : tuples) {
    // sLogger.info("Tuple: " + tuple);
    // _input.clear();
    // // _input.put(Utils.kCPU, tuple.getIntegerByField(Utils.kCPU));
    // _input.put(Utils.kMemory, tuple.getValueByField(Utils.kMemory));
    // _input.put(Utils.kDeviceId, tuple.getStringByField(Utils.kDeviceId));
    // _input.put(Utils.kTimeStamp,
    // tuple.getValueByField(Utils.kTimeStamp));
    //
    // try {
    // executeScript_();
    // } catch (ResourceException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // } catch (ScriptException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // }
    // }

    private void readObject(ObjectInputStream in) throws IOException,
                                                 ClassNotFoundException {
        in.defaultReadObject();

        init_();
    }
}
