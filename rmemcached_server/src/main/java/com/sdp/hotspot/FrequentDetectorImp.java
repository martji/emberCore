package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by magq on 16/1/12.
 */
public class FrequentDetectorImp implements BaseFrequentDetector {

    private int frequentItemsNumber = 1;

    private static FrequentDetectorImp ourInstance = null;

    public static FrequentDetectorImp getInstance() {
        if (ourInstance == null) {
            ourInstance = new FrequentDetectorImp();
        }
        return ourInstance;
    }

    private FrequentDetectorImp() {
        initConfig();
    }

    public void initConfig() {
        frequentItemsNumber = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.FREQUENT_ITEM_NUMBER);
    }

    /**
     * Frequent algorithm
     */
    public boolean registerItem(String key) {
        boolean result = false;
        if(itemCounters.containsKey(key)) {
            itemCounters.put(key, itemCounters.get(key) + 1);
            result = true;
        } else if(itemCounters.size() < frequentItemsNumber) {
            itemCounters.put(key, 1);
        } else {
            String str = null;
            Iterator iter = itemCounters.keySet().iterator();
            while(iter.hasNext()){
                str = (String) iter.next();
                if (itemCounters.get(str) > 0) {
                    itemCounters.put(str, itemCounters.get(str) - 1);
                } else {
                    itemCounters.remove(str);
                }
            }
        }
        return result;
    }

    public ConcurrentHashMap<String, Integer> getItemCounters() {
        return itemCounters;
    }
}
