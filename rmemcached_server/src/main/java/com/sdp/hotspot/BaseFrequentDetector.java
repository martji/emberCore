package com.sdp.hotspot;

/**
 * Created by magq on 16/1/12.
 */
public interface BaseFrequentDetector {

    /**
     * 读取配置
     */
    public void initConfig();

    /**
     *
     * @param key
     * @return 插入元素是否是frequent item
     */
    public boolean registerItem(String key);
}
