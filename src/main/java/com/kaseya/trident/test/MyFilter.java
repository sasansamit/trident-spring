package com.kaseya.trident.test;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class MyFilter extends BaseFilter {

    /**
     * 
     */
    private static final long serialVersionUID = 975621734663618595L;

    public boolean isKeep(TridentTuple tuple) {
        // return tuple.getString(0).equalsIgnoreCase("the");
        return true;
    }

}
