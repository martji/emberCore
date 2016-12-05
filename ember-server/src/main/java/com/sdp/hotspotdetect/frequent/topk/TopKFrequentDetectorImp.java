package com.sdp.hotspotdetect.frequent.topk;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 2005-ICDT-Efficient Computation of Frequent and Top-k Elements in Data Streams
 *         这个和TopKDetectorImp的区别是：那篇论文里面有两种判定热点数据的方法。
 *         TopKFrequentDetectorImp是根据频率来判断的，TopKDetectorImp是选前K’个数据.
 *         这个java文件根据频率来判断热点数据，算法伪代码的图在整理文件里。这里和他的原算法有一点不一样，
 *         我是用(counterMap.get(key) - preValue.get(key) >= hotSpotPercentage * itemSum)这个判断，
 *         原算法是CounterMap.get(key) >= hotSpotPercentage * itemSum
 *         原算法没有对itemSum的更新，所以我也没有更新。
 */

public class TopKFrequentDetectorImp implements FrequentDetectorInterface {

    private int counterNumber;
    private double hotSpotPercentage;

    private ConcurrentHashMap<String, Integer> preValue = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, Integer> counterMap = new ConcurrentHashMap<String, Integer>();

    public int itemSum = 0;
    private int preItemSum = 0;

    public TopKFrequentDetectorImp() {
        initConfig();
    }

    public void initConfig() {
        counterNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.COUNTER_NUMBER);
        hotSpotPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);

        Log.log.info("[TopK Frequent] " + "counterNumber = " + counterNumber
                + ", hotSpotPercentage = " + hotSpotPercentage);
    }

    public boolean registerItem(String key) {
        itemSum++;
        boolean result = false;
        if (counterMap.containsKey(key)) {
            counterMap.put(key, counterMap.get(key) + 1);
            if (counterMap.get(key) - preValue.get(key) >= hotSpotPercentage * Math.max(preItemSum, itemSum)) {
                currentHotSpotCounters.put(key, counterMap.get(key) - preValue.get(key));
                result = true;
            }
        } else if (counterMap.size() < counterNumber) {
            counterMap.put(key, 1);
            preValue.put(key, 0);
        } else {
            int min = Integer.MAX_VALUE;
            String strMin = null;
            Set<String> keys = new HashSet<String>(counterMap.keySet());
            for (String str : keys) {
                if (counterMap.get(str) < min) {
                    strMin = str;
                    min = counterMap.get(strMin);
                }
            }
            counterMap.remove(strMin);
            preValue.remove(strMin);
            counterMap.put(key, min + 1);
            preValue.put(key, min);
        }
        return result;
    }

    public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
        return currentHotSpotCounters;
    }

    public void resetCounter() {
        currentHotSpotCounters.clear();
    }

    public String updateHotSpot() {
        itemSum -= preItemSum;
        preItemSum = itemSum;
        return null;
    }

}
