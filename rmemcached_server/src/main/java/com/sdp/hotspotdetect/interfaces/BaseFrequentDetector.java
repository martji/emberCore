package com.sdp.hotspotdetect.interfaces;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by magq on 16/1/12.
 */
public interface BaseFrequentDetector {

    ConcurrentHashMap<String, Integer> itemCounters = new ConcurrentHashMap<String, Integer>();

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


    ConcurrentHashMap<String, Integer> getItemCounters();
    
    void resetCounter();

    String updateItemSum();
    void refreshSWFPCounter();
}
