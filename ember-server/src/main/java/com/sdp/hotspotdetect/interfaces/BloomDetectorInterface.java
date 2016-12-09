package com.sdp.hotspotdetect.interfaces;

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
     * @return the index array of the key
     */
    int[] getHashIndex(String key);

    /**
     * @param key
     */
    boolean registerItem(String key);

    void updateFilterThreshold();

    void resetCounter();

}
