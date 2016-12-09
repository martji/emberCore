package com.sdp.hotspotdetect.frequent.topk;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 2005-ICDT-Efficient Computation of Frequent and Top-k Elements in Data Streams
 *         1.采用m个counter对m个item进行监管，相应的counter有一个ε值，对应于代码中的preValue。
 *         2.当计数器更换监管item时，用ε记录下新item到来前的counter的值。计数器当前监管的item的数据项个数肯定大于Counter-ε，充分性
 *         3.处理过程：
 *         如果新到来的item已经被监管，则相应计数器加一
 *         如果新到来的item没有被监管，暂且相信它为频繁访问的item，找出当前计数器中值最小的item进行替换。该counter的值赋予ε，counter++
 *         4.判定过程：寻找top-k，实际找到的item个数为k’
 *         5.topElementsList中存储了热点数据，topElementsList数组两秒更新一次
 */

public class TopKDetectorImp implements FrequentDetectorInterface {

    private int topItemNumber = 10;
    private int counterNumber = 20;

    private ConcurrentHashMap<String, Integer> preValue = new ConcurrentHashMap<String, Integer>();
    private ArrayList<String> topElementsList = new ArrayList<>();

    public TopKDetectorImp() {
        initConfig();
    }

    public void initConfig() {
        topItemNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.TOP_ITEM_NUMBER);
        counterNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.COUNTER_NUMBER);

        Log.log.info("[TopK] " + "counterNumber = " + counterNumber
                + ", topItemNumber = " + counterNumber);
    }

    public boolean registerItem(String key) {
        if (currentHotSpotCounters.containsKey(key)) {
            currentHotSpotCounters.put(key, currentHotSpotCounters.get(key) + 1);
        } else if (currentHotSpotCounters.size() < counterNumber) {
            currentHotSpotCounters.put(key, 1);
            preValue.put(key, 0);
        } else {
            int min = Integer.MAX_VALUE;
            String strMin = null;
            Set<String> keys = new HashSet<String>(currentHotSpotCounters.keySet());
            for (String str : keys) {
                if (currentHotSpotCounters.get(str) < min) {
                    strMin = str;
                    min = currentHotSpotCounters.get(str);
                }
            }
            currentHotSpotCounters.remove(strMin);
            preValue.remove(strMin);
            currentHotSpotCounters.put(key, min + 1);
            preValue.put(key, min);
        }

        if (topElementsList.contains(key)) {
            return true;
        }
        return false;
    }

    /**
     * update the topElementsList
     */
    public void updateTopKList() {
        ArrayList<String> list = new ArrayList<>();
        ArrayList<String> keys = new ArrayList<String>(currentHotSpotCounters.keySet());
        Collections.sort(keys, new Comparator<String>() {
            public int compare(String str1, String str2) {
                try {
                    return currentHotSpotCounters.get(str2) - currentHotSpotCounters.get(str1);
                } catch (Exception e) {
                }
                return 0;
            }
        });
        int i, minFrequent = Integer.MAX_VALUE;
        String str;
        for (i = 0; i < topItemNumber; i++) {
            str = keys.get(i);
            list.add(str);
            if (currentHotSpotCounters.get(str) - preValue.get(str) < minFrequent) {
                minFrequent = currentHotSpotCounters.get(str) - preValue.get(str);
            }
        }
        if (currentHotSpotCounters.get(keys.get(i)) >= minFrequent) {
            for (i = topItemNumber + 1; i < keys.size(); i++) {
                str = keys.get(i - 1);
                list.add(str);
                if (currentHotSpotCounters.get(str) - preValue.get(str) < minFrequent) {
                    minFrequent = currentHotSpotCounters.get(str) - preValue.get(str);
                }
                if (currentHotSpotCounters.get(keys.get(i)) < minFrequent) {
                    break;
                }
            }
        }
        topElementsList = list;
    }

    public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<String, Integer>();
        for (String key : topElementsList) {
            map.put(key, currentHotSpotCounters.get(key) - preValue.get(key));
        }
        return map;
    }

    public void resetCounter() {
        currentHotSpotCounters.clear();
    }

    public void updateThreshold() {
        updateTopKList();
    }

}
