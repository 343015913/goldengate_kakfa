package com.rogers.goldengate.exceptions;



public class ConfigException extends RuntimeException {


    public ConfigException(String msg) {
        super(msg);
    }

    public ConfigException(String msg, Throwable cause) {
        super(msg, cause);
    }

}