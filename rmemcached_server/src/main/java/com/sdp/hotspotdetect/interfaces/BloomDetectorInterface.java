package com.sdp.hotspotdetect.interfaces;

import java.util.Vector;

/**
 * Created by magq on 16/1/12.
 */
public interface BloomDetectorInterface {

    /**
     * read config
     */
    void initConfig();

    /**
     * @param key
     * @return the indexs of the key
     */
    int[] getHashIndex(String key);

    /**
     * @param key
     */
    boolean registerItem(String key);

    /**
     * reset the counter period
     */
    void resetBloomCounters();

    /**
     * @return hot spots
     */
    Vector<String> getHotSpots();

    String updateFilterThreshold();
    void resetCounter();
    int getItemSum();

}
