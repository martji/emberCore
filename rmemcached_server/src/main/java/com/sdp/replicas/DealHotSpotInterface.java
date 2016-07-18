package com.sdp.replicas;

/**
 * Created by Guoqing on 2016/1/13.
 */
public interface DealHotSpotInterface {

    /**
     * create replica for hot spot set
     */
    void dealHotData();

    /**
     * retire replica of cold items
     */
    void dealColdData();

    /**
     * deal single hot spot
     * @param key
     */
    void dealHotData(String key);
}
