package com.sdp.replicas;

/**
 * Created by Guoqing on 2016/1/13.
 */
public interface DealHotSpotInterface {

    /**
     * create replica for hotspot
     */
    void dealHotData();

    /**
     * retire replica of cold items
     */
    void dealColdData();

    void dealHotData(String key);
}
