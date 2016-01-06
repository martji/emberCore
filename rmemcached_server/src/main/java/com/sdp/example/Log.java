package com.sdp.example;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
/**
 * 
 * @author martji
 *
 */
public class Log {
	public static Logger log;
	public static int id = -1;
	
	public static void init(String path) {
		PropertyConfigurator.configure(path);
		log = Logger.getLogger( MServerMain.class.getName());
	}
	
	public static void init() {
		PropertyConfigurator.configure(System.getProperty("user.dir") + "/config/log4j.properties");
		log = Logger.getLogger( MServerMain.class.getName());
	}

	public static void setInstanceId(int id) {
		Log.id = id;
	}
}
