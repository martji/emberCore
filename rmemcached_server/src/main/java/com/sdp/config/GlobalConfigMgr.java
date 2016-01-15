package com.sdp.config;

import com.sdp.example.Log;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by magq on 16/1/14.
 */
public class GlobalConfigMgr {

    public static HashMap<String, Object> configMap = new HashMap<String, Object>();

    public void init() {
        String configPath = System.getProperty("user.dir") + "/config/config.properties";
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(configPath));

            int multi_bloom_filter_number = Integer.decode(properties.getProperty(MULTI_BLOOM_FILTER_NUMBER, "4"));
            int bloom_filter_length = Integer.decode(properties.getProperty(BLOOM_FILTER_LENGTH, "100"));
            String monitor_address = properties.getProperty(MONITOR_ADDRESS, "127.0.0.1").toString();
            int hotspot_threshold = Integer.decode(properties.getProperty(HOTSPOT_THRESHOLD, "100"));
            int slice_time = Integer.decode(properties.getProperty(SLICE_TIME, "15")) * 1000;
            int counter_peroid = Integer.decode(properties.getProperty(COUNTER_PERIOD, "10"));
            int frequent_item_number = Integer.decode(properties.getProperty(FREQUENT_ITEM_NUMBER, "10")) * 1000;

            configMap.put(MULTI_BLOOM_FILTER_NUMBER, multi_bloom_filter_number);
            configMap.put(BLOOM_FILTER_LENGTH, bloom_filter_length);
            configMap.put(MONITOR_ADDRESS, monitor_address);
            configMap.put(HOTSPOT_THRESHOLD, hotspot_threshold);
            configMap.put(SLICE_TIME, slice_time);
            configMap.put(COUNTER_PERIOD, counter_peroid);
            configMap.put(FREQUENT_ITEM_NUMBER, frequent_item_number);
        } catch (Exception e) {
            Log.log.error("wrong config.properties", e);
        }
    }

    public static final String MULTI_BLOOM_FILTER_NUMBER = "multi_bloom_filter_number";
    public static final String BLOOM_FILTER_LENGTH = "bloom_filter_length";
    public static final String MONITOR_ADDRESS = "monitor_address";
    public static final String HOTSPOT_THRESHOLD = "hotspot_threshold";
    public static final String SLICE_TIME = "slice_time";
    public static final String FREQUENT_ITEM_NUMBER = "frequent_item_number";
    public static final String COUNTER_PERIOD = "counter_period";
}
