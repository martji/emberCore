package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;

import java.util.Iterator;
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
        initConfig();
    }

    public void initConfig() {
        frequentItemsNumber = (Integer) GlobalConfigMgr.configMap.get(GlobalConfigMgr.FREQUENT_ITEM_NUMBER);
    }

    public boolean registerItem(String key) {
        if(itemCounters.containsKey(key)) {
            itemCounters.replace(key, itemCounters.get(key) + 1);
        } else if(itemCounters.size() < frequentItemsNumber) {
            itemCounters.put(key, 1);
        } else if(itemCounters.containsValue(0)) {
            String str = null;
            Iterator iter = itemCounters.keySet().iterator();
            while(iter.hasNext()) {
                str = (String) iter.next();
                if(itemCounters.get(str) == 0){
                    itemCounters.remove(str, 0);
                    itemCounters.put(key, 1);
                    break;
                }
            }
        } else {
            String str = null;
            Iterator iter = itemCounters.keySet().iterator();
            while(iter.hasNext()){
                str = (String) iter.next();
                itemCounters.replace(str, itemCounters.get(str) - 1);
            }
        }
        if (itemCounters.containsKey(key)) {
            return true;
        }
        else {
            return false;
        }
    }
}
