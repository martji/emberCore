package com.sdp.hotspot;

public interface BaseEcDetector {
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
