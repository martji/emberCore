package com.sdp.example;

import com.sdp.common.RegisterHandler;
import com.sdp.config.GlobalConfigMgr;
import com.sdp.server.MServer;
import com.sdp.server.MServerHandler;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.Scanner;
import java.util.Vector;

/**
 * 
 * @author martji
 *
 */
public class MServerMain {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MServerMain emberLauncher = new MServerMain();
		emberLauncher.start();
	}

	public void start() {
        GlobalConfigMgr.init();
		RegisterHandler.initHandler();
		int id = getMemcachedNumber();
        GlobalConfigMgr.setId(id);
		Log.setInstanceId(id);
		Log.init();

		Log.log.info("New r-memcached instance start, instance id: " + id);
		
		MServer mServer = new MServer();
		ConcurrentHashMap<String, Vector<Integer>> replicasIdMap = new ConcurrentHashMap<String, Vector<Integer>>();
		MServerHandler wServerHandler = new MServerHandler(replicasIdMap);
		MServerHandler rServerHandler = new MServerHandler(replicasIdMap);
		rServerHandler.setMServer(mServer);
		rServerHandler.initMessageManager();
		mServer.init(wServerHandler, rServerHandler);
	}
	
	public int getMemcachedNumber() {
		System.out.print("Please input the server number:");
		Scanner scanner = new Scanner(System.in);
		return Integer.decode(scanner.next());
	}
}
