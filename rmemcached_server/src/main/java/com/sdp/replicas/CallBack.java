package com.sdp.replicas;

/**
 * Created by Guoqing on 2016/1/13.
 */
public interface CallBack {

    /**
     * create replica for hotspot
     */
    public void dealHotData();

    /**
     * retire replica of cold items
     */
    public void dealColdData();
}
