package com.sdp.utils;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SpotUtil {

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
    public static ConcurrentLinkedQueue<String> candidateColdSpots = new ConcurrentLinkedQueue<>();
    public static ConcurrentLinkedQueue<String> periodHotSpots = new ConcurrentLinkedQueue<>();
    public static AtomicInteger coldSpotNumber = new AtomicInteger(0);
    public static AtomicInteger hotSpotNumber = new AtomicInteger(0);

    public static double retireRatio;

    public static void reset(ConcurrentHashMap<String, Vector<Integer>> map) {
        retireRatio = 0;
        if (hotSpotNumber.get() != 0) {
            retireRatio = (double) coldSpotNumber.get() / hotSpotNumber.get();
        }

        candidateColdSpots.clear();
        candidateColdSpots.addAll(map.keySet());
        periodHotSpots.clear();
        coldSpotNumber.set(0);
        hotSpotNumber.set(0);
    }
}
