package com.sdp.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author martji
 *         Global log config, where the log file id is the server id.
 */
public class Log {
    public static Logger log;
    public static int id = -1;

    public static void init(String path) {
        System.setProperty("log4j.configurationFile", path);
        log = LogManager.getRootLogger();
    }

    public static void init() {
        String path = System.getProperty("user.dir") + "/config/log4j2.xml";
        init(path);
    }

    public static void setInstanceId(int id) {
        Log.id = id;
        System.setProperty("server.id", id + "");
    }
}
