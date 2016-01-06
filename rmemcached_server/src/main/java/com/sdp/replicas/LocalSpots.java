package com.sdp.replicas;

import org.jboss.netty.util.internal.ConcurrentHashMap;

public class LocalSpots {
	public static ConcurrentHashMap<String, String> hotspots = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, String> coldspots = new ConcurrentHashMap<String, String>();
	private static final String table = "usertable:user";
	
	public static int threshold = 10000;
	public static int coldThreshold = 1;
	
	public static boolean containsHot(String key) {
		return hotspots.containsKey(key);
	}
	
	public static void addHot(Integer keynum) {
		hotspots.put(table + keynum, "");
	}

	public static void addHot(String key) {
		hotspots.put(key, "");
	}

	public static void removeHot(String key) {
		hotspots.remove(key);
	}
	
	public static boolean containsCold(String key) {
		return coldspots.containsKey(key);
	}
	
	public static void addCold(Integer keynum) {
		coldspots.put(table + Utils.hash(keynum), "");
	}

	public static void addCold(String key) {
		coldspots.put(key, "");
	}

	public static void removeCold(String key) {
		coldspots.remove(key);
	}
}
