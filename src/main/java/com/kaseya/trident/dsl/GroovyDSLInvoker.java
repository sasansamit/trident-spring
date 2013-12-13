package com.kaseya.trident.dsl;

import groovy.lang.Binding;
import groovy.util.GroovyScriptEngine;
import groovy.util.ResourceException;
import groovy.util.ScriptException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import com.kaseya.trident.Utils;

public class GroovyDSLInvoker implements StateUpdater<MemoryStateObject> {
    private GroovyScriptEngine _scriptEngine;
    private Binding _binding;
    private CompilerConfiguration _config;
    private SecureASTCustomizer _security;
    private File _file;
    private HashMap<String, Object> _input;

    public GroovyDSLInvoker(final String filePath) throws IOException {
        this._file = new File(filePath);
        this._scriptEngine = new GroovyScriptEngine(_file.getParent());
        this._binding = new Binding();
        this._config = new CompilerConfiguration();
        this._security = new SecureASTCustomizer();
        this._input = new HashMap<String, Object>();

        List<String> importsWhiteList = new ArrayList<String>();
        importsWhiteList.add("java.lang.Math");
        _security.setStarImportsWhitelist(importsWhiteList);
        _config.addCompilationCustomizers(_security);
        _scriptEngine.setConfig(_config);
    }

    public void executeScript_() throws ResourceException, ScriptException {
        _scriptEngine.run(_file.getAbsolutePath(), _binding);
    }

    public void prepare(Map conf, TridentOperationContext context) {
    }

    public void cleanup() {
    }

    public void updateState(MemoryStateObject state,
            List<TridentTuple> tuples,
            TridentCollector collector) {

        _binding.setVariable("dataStore", state);

        for (TridentTuple tuple : tuples) {
            _input.clear();
            _input.put(Utils.kCPU, tuple.getIntegerByField(Utils.kCPU));
            _input.put(Utils.kMemory, tuple.getIntegerByField(Utils.kMemory));
            _input.put(Utils.kDeviceId, tuple.getStringByField(Utils.kDeviceId));
            _input.put(Utils.kTimeStamp, tuple.getIntegerByField(Utils.kTimeStamp));

            _binding.setVariable("input", _input);

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
}
