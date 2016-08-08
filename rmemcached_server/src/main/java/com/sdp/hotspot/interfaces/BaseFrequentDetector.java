package com.sdp.hotspot.interfaces;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by magq on 16/1/12.
 */
public interface BaseFrequentDetector {

    public ConcurrentHashMap<String, Integer> itemCounters = new ConcurrentHashMap<String, Integer>();

    /**
     * 读取配置
     */
    public void initConfig();

    /**
     *
     * @param key
     * @return 插入元素是否是frequent item
     */
    public boolean registerItem(String key, int presum);


    public ConcurrentHashMap<String, Integer> getItemCounters();
    
    public void resetCounter();
}
