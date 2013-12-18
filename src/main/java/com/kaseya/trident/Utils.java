package com.kaseya.trident;

import java.util.List;

import com.kaseya.trident.operations.IStreamOperation;

public class Utils {

    public static final String kCPU = "cpu";
    public static final String kMemory = "memory";
    public static final String kDeviceId = "deviceid";
    public static final String kTimeStamp = "timestamp";

    public static void ValidateOperationType(IStreamOperation operation,
            Object obj,
            Class<?>... expectedTypes) {
        boolean success = false;
        for (Class<?> cls : expectedTypes) {
            if (cls.isInstance(obj)) {
                success = true;
                break;
            }
        }
        if (!success) {
            String msg = operation.getClass()
                    + " can only be called on a [";
            for (Class<?> cls : expectedTypes) {
                msg += "\n - " +cls;
            }
            msg += "\n]. Current stream is of type ["
                    + obj.getClass()
                    + "]";
            throw new RuntimeException(msg);
        }
    }

    public static void ValidateArgs(final String[] args) {

        if (args.length < 1) {
            throw new IllegalArgumentException("Arguments: Needs one argument - <XmlApplicationContext>");
        }
    }
}
