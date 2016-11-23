package com.sdp.manager.hotspotmanager.interfaces;

/**
 * Created by Guoqing on 2016/1/13.
 */
public interface DealHotSpotInterface {

    /**
     * create replica for hot spot set
     */
    void dealHotData();

    /**
     * deal single hot spot
     * @param key
     */
    void dealHotData(String key);

    /**
     * retire replica of cold items
     */
    void dealColdData();
}
