package com.idreamsky.dc.blink.common.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author colby.luo
 * @date 2020/3/27 13:27
 */
public class GLoggerFactory {

    private final static Map<String, GLogger> CACHED = new HashMap<>();

    public static GLogger getGLogger(){
        String className = Thread.currentThread().getStackTrace()[1].getClassName();
        return getGLogger(className);
    }

    public static GLogger getGLogger(Class<?> clazz) {
        return getGLogger(clazz.getName());
    }

    public static synchronized GLogger getGLogger(String loggerName) {
        GLogger gLogger = CACHED.get(loggerName);
        if (gLogger == null) {
            gLogger = new GLogger(loggerName);
            CACHED.put(loggerName, gLogger);
        }
        return gLogger;
    }
}
