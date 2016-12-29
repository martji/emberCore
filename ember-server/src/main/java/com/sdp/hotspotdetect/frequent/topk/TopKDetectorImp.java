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

    private int topItemNumber;
    private int counterNumber;

    private ConcurrentHashMap<String, TopKCounter> hotSpotMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> topElements = new ConcurrentHashMap<>();

    public TopKDetectorImp() {
        initConfig();
    }

    public void initConfig() {
        topItemNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.TOP_ITEM_NUMBER);
        counterNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.COUNTER_NUMBER);

        Log.log.info("[TopK] " + "counterNumber = " + counterNumber
                + ", topItemNumber = " + topItemNumber);
    }

    public boolean registerItem(String key) {
        if (hotSpotMap.containsKey(key)) {
            hotSpotMap.get(key).add();
        } else if (hotSpotMap.size() < counterNumber) {
            hotSpotMap.put(key, new TopKCounter(1, 0));
        } else {
            int min = Integer.MAX_VALUE;
            String strMin = null;
            Set<String> keys = new HashSet<String>(hotSpotMap.keySet());
            for (String str : keys) {
                TopKCounter counter = hotSpotMap.get(str);
                if (counter != null && counter.value < min) {
                    strMin = str;
                    min = counter.value;
                }
            }
            if (strMin != null) {
                hotSpotMap.remove(strMin);
                hotSpotMap.put(key, new TopKCounter(min + 1, min));
            }
        }

        if (topElements.containsKey(key)) {
            return true;
        }
        return false;
    }

    /**
     * update the topElements
     */
    public void updateTopK() {
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();

        final ConcurrentHashMap<String, TopKCounter> counters = new ConcurrentHashMap<>(hotSpotMap);
        ArrayList<String> keys = new ArrayList<String>(counters.keySet());
        if (keys.size() <= 0) {
            return;
        }
        Collections.sort(keys, new Comparator<String>() {
            public int compare(String str1, String str2) {
                try {
                    return counters.get(str2).compareTo(counters.get(str1));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });

        int len = Math.min(topItemNumber, keys.size());
        int i, minFrequent = Integer.MAX_VALUE;
        String str;
        int count;
        for (i = 0; i < len; i++) {
            str = keys.get(i);
            count = counters.get(str).getCount();
            map.put(str, count);
            minFrequent = Math.min(count, minFrequent);
        }
        if (i < keys.size() && counters.get(keys.get(i)).getCount() >= minFrequent) {
            for (i = len + 1; i < keys.size(); i++) {
                str = keys.get(i - 1);
                count = counters.get(str).getCount();
                map.put(str, count);
                minFrequent = Math.min(count, minFrequent);
                if (counters.get(keys.get(i)).getCount() < minFrequent) {
                    break;
                }
            }
        }
        topElements = map;
    }

    public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
        return topElements;
    }

    public void resetCounter() {
        currentHotSpotCounters.clear();
        hotSpotMap.clear();
    }

    public void updateThreshold() {
        updateTopK();
    }

    public class TopKCounter implements Comparable<TopKCounter> {
        private int value;
        private int preValue;

        private TopKCounter(int value, int preValue) {
            this.value = value;
            this.preValue = preValue;
        }

        public void add() {
            value++;
        }

        public int getCount() {
            return value - preValue;
        }

        @Override
        public int compareTo(TopKCounter that) {
            return that.value - this.value;
        }
    }

}
