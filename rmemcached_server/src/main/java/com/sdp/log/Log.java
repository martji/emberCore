package com.sdp.log;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * @author martji
 * Global log config, where the log file id is the server id.
 */
public class Log {
	public static Logger log;
	public static int id = -1;
	
	public static void init(String path) {
		PropertyConfigurator.configure(path);
		log = Logger.getRootLogger();
	}
	
	public static void init() {
		String path = System.getProperty("user.dir") + "/config/log4j.properties";
		init(path);
	}

	public static void setInstanceId(int id) {
		Log.id = id;
		System.setProperty("config.id", id + "");
	}
}
