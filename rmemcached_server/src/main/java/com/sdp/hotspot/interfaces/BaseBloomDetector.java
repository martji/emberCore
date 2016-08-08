package com.sdp.hotspot.interfaces;

import java.util.Vector;

/**
 * Created by magq on 16/1/12.
 */
public interface BaseBloomDetector {

    /**
     * 初始化配置
     */
    public void initConfig();

    /**
     * @param key
     * @return the indexs of the key
     */
    public int[] getHashIndex(String key);

    /**
     * @param key
     */
    public boolean registerItem(String key);

    /**
     * 周期性重置计数器
     */
    public void resetBloomCounters();

    /**
     * @return 获取热点数据
     */
    public Vector<String> getHotSpots();
}
