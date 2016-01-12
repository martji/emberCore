package com.sdp.hotspot;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by magq on 16/1/12.
 */
public class FrequentDetectorImp implements BaseFrequentDetector{

    private int frequentItemsNumber = 1;
    private ConcurrentHashMap<String, Integer> itemCounters = new ConcurrentHashMap<String, Integer>();

    private static FrequentDetectorImp ourInstance = null;

    public static FrequentDetectorImp getInstance() {
        if (ourInstance == null) {
            ourInstance = new FrequentDetectorImp();
            ourInstance.initConfig();
        }
        return ourInstance;
    }

    private FrequentDetectorImp() {

    }

    public void initConfig() {

    }

    public boolean registerItem(String key) {
        return false;
    }
}
