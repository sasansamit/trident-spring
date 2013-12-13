package com.kaseya.trident;

import com.kaseya.trident.operations.IStreamOperation;

public class Utils {

    public static final String kCPU = "cpu";
    public static final String kMemory = "memory";
    public static final String kDeviceId = "deviceid";
    public static final String kTimeStamp = "timestamp";

    public static void ValidateOperationType(IStreamOperation operation,
            Object obj,
            Class<?> expectedType) {
        if (!expectedType.isInstance(obj)) {
            throw new RuntimeException(operation.getClass()
                                       + " can only be called on a "
                                       + expectedType
                                       + ". Current stream is of type ["
                                       + obj.getClass()
                                       + "]");
        }
    }

    public static void ValidateArgs(final String[] args) {

        if (args.length < 2) {
            throw new IllegalArgumentException("Arguments: Needs two arguments - <XmlApplicationContext> <TopologySubmission bean name>");
        }
        else if (args[0] == null) {
            throw new IllegalArgumentException("Argument 1: XmlApplicationContext was not defined");
        } else if (args[1] == null) {
            throw new IllegalArgumentException("Argument 2: TopologySubmission bean was not defined");
        }
    }
}
