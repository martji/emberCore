package com.sdp.hotspotdetect.frequent;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @deprecated
 */
public class EcDetectorImp extends Thread implements FrequentDetectorInterface {

    private double frequentPercentage = 0.0001;
    private double errorRate = 0.01;
    private int counterNumber = 10000;

    public int itemSum = 0;

    public ConcurrentHashMap<String, Integer> keyCounters = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, Integer> keyPreCounters = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, Integer> keySumCounters = new ConcurrentHashMap<String, Integer>();

    public EcDetectorImp() {
        initConfig();
    }

    public void initConfig() {
        frequentPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);
        errorRate = (Double) ConfigManager.propertiesMap.get(ConfigManager.ERROR_RATE);

        Log.log.info("[EC] frequentPercentage = " + frequentPercentage
                + ", errorRate = " + errorRate);
    }

    /**
     * 算法伪代码在整理文件里
     *
     * @param key
     * @return
     */
    public boolean registerItem(String key) {
        itemSum++;

        if (keyCounters.containsKey(key)) {
            keyCounters.put(key, keyCounters.get(key) + 1);
        } else if (keyCounters.size() < counterNumber) {
            keyCounters.put(key, 1);
            keyPreCounters.put(key, 0);
            keySumCounters.put(key, itemSum);
        } else {
            while (!keyCounters.containsValue(0)) {
                Iterator iterator = keyCounters.keySet().iterator();
                String str;
                while (iterator.hasNext()) {
                    str = (String) iterator.next();
                    keyCounters.put(str, keyCounters.get(str) - 1);
                    keyPreCounters.put(str, keyPreCounters.get(str) + 1);
                }
            }
            Iterator iterator = keyCounters.keySet().iterator();
            String str;
            while (iterator.hasNext()) {
                str = (String) iterator.next();
                if (keyCounters.get(str) == 0) {
                    keyCounters.remove(str);
                    keyPreCounters.remove(str);
                    keySumCounters.remove(str);
                }
            }
            keyCounters.put(key, 1);
            keyPreCounters.put(key, 0);
            keySumCounters.put(key, itemSum);
        }
        //itemSum是一个变量
        if (keyCounters.get(key) + keyPreCounters.get(key) > (frequentPercentage - errorRate) * itemSum) {
            currentHotSpotCounters.put(key, keyCounters.get(key) + keyPreCounters.get(key));
            return true;
        }
        return false;
    }

    public void updateItemsum(int preSum) {
        itemSum -= preSum;
    }

    public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
        return currentHotSpotCounters;
    }

    public void resetCounter() {
        currentHotSpotCounters.clear();
    }

    public String updateHotSpot() {
        return null;
    }

}
