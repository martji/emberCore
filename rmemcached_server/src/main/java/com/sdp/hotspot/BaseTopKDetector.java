package com.sdp.hotspot;

public interface BaseTopKDetector {
	
	 /**
     * 读取配置
     */
    public void initConfig();

    /**
     *
     * @param key
     * @return 插入元素是否是Top-k item
     */
    public boolean registerItem(String key);
}
