package com.sdp.hotspotdetect.interfaces;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by magq on 16/1/12.
 */
public interface FrequentDetectorInterface {

    ConcurrentHashMap<String, Integer> currentHotSpotCounters = new ConcurrentHashMap<String, Integer>();

    /**
     * read config
     */
    void initConfig();

    /**
     *
     * @param key
     * @return decide whether the key is an frequent item
     */
    boolean registerItem(String key, int preSum);

    ConcurrentHashMap<String, Integer> getCurrentHotSpot();
    
    void resetCounter();
    String updateFrequentCounter();
}
