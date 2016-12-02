package com.sdp.replicas;

import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicInteger;

public class LocalSpots {

    /**
     * @deprecated
     */
    public static ConcurrentHashMap<String, String> hotSpots = new ConcurrentHashMap<String, String>();
    /**
     * @deprecated
     */
    public static ConcurrentHashMap<String, String> coldSpots = new ConcurrentHashMap<String, String>();
    /**
     * @deprecated
     */
    private static final String table = "usertable:user";

    public static boolean containsHot(String key) {
        return hotSpots.containsKey(key);
    }

    public static void addHot(Integer keyNum) {
        hotSpots.put(table + keyNum, "");
    }

    public static void addHot(String key) {
        hotSpots.put(key, "");
    }

    public static void removeHot(String key) {
        hotSpots.remove(key);
    }

    public static boolean containsCold(String key) {
        return coldSpots.containsKey(key);
    }

    public static void addCold(Integer keyNum) {
        coldSpots.put(table + Utils.hash(keyNum), "");
    }

    public static void addCold(String key) {
        coldSpots.put(key, "");
    }

    public static void removeCold(String key) {
        coldSpots.remove(key);
    }


    /**
     * Record hot spots and cold spots in current period.
     */
    public static ConcurrentHashMap<String, Object> candidateColdSpots = new ConcurrentHashMap<String, Object>();
    public static AtomicInteger hotSpotNumber = new AtomicInteger(0);
}
