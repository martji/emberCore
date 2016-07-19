package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.example.Log;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Guoqing on 2016/3/29.
 */
public class SWFPDetectorImp implements BaseFrequentDetector {

    /**
     * The really counter to count the visit times of item.
     */
    private ConcurrentHashMap<String, SWFPCounter> CounterMap = new ConcurrentHashMap<String, SWFPCounter>();

    /**
     * The threshold of hot spot frequent percentage, which is p.
     */
    private double hotSpotPercentage = 0.0001;

    /**
     * The default influence of hot spot, which is P, this parameter does not change.
     */
    private static double HOT_SPOT_INFLUENCE = 0.1;
    private static double HOT_SPOT_PERCENTAGE = 0.0001;


    private int counterNumber;
    public int itemSum = 0;
    private int preItemSum = 0;

    public SWFPDetectorImp() {
        initConfig();
    }

    public void initConfig() {
        HOT_SPOT_PERCENTAGE = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOT_SPOT_PERCENTAGE);
        HOT_SPOT_INFLUENCE = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOT_SPOT_INFLUENCE);
        Log.log.info("[frequent counter parameters]: " + "p = " + hotSpotPercentage + ", P = " + HOT_SPOT_INFLUENCE);

        hotSpotPercentage = HOT_SPOT_PERCENTAGE;
    	counterNumber = (int) (1 / hotSpotPercentage);
    }

    /**
     *
     * @param key
     * @param bloomFilterSum
     * @return whether the item key is hot spot.
     */
    public boolean registerItem(String key, int bloomFilterSum) {
        itemSum ++;

        if (CounterMap.containsKey(key)) {
            CounterMap.get(key).add();
            int threshold = (int)(hotSpotPercentage * (itemSum > bloomFilterSum ? itemSum : bloomFilterSum));
            if (CounterMap.get(key).getCount() > threshold) {
            	itemCounters.put(key, CounterMap.get(key).getReallyCount());
            }
        } else {
            if (CounterMap.size() < counterNumber) {
                CounterMap.put(key, new SWFPCounter(key));
            } else {
                Set<String> keySet = CounterMap.keySet();
                for (String item: keySet) {
                    CounterMap.get(item).del();

                    if (CounterMap.get(item).frequent <= 0) {
                        CounterMap.remove(item);
                    }
                }

                if (CounterMap.size() < counterNumber) {
                    CounterMap.put(key, new SWFPCounter(key));
                }
            }
        }

        if (itemCounters.containsKey(key)) {
            return true;
        }
        return false;
    }

    /**
     * Adjust hotSpotPercentage, the workload influence of hot spot must larger
     * than HOT_SPOT_INFLUENCE.
     */
    public String updateItemSum() {
        itemSum -= preItemSum;
        preItemSum = itemSum;

        ArrayList<Integer> hotSpots = new ArrayList<Integer>(itemCounters.values());
        int totalCount = 0;
        for (int i = 0; i < hotSpots.size(); i++) {
            totalCount += hotSpots.get(i);
        }
        double tmp = (double) totalCount / itemSum;
        if (totalCount > 0 && tmp < HOT_SPOT_INFLUENCE) {
            hotSpotPercentage /= 2;
        } else if (totalCount == 0) {
            hotSpotPercentage = HOT_SPOT_PERCENTAGE;
        }
        counterNumber = (int) (1 / hotSpotPercentage);

        String result =  "  |  [frequent counter]: " + totalCount + " / "+ itemSum +
                " [hot_spot_percentage]: " + hotSpotPercentage;
        return result;
    }

    public void resetCounter() {
        itemCounters.clear();
    }

    public void refreshSWFPCounter() {
        Set<String> keySet = CounterMap.keySet();
        for (String item: keySet) {
            CounterMap.get(item).refresh();
        }
    }

    public ConcurrentHashMap<String, Integer> getItemCounters() {
        return itemCounters;
    }

    static class SWFPCounter implements Comparable<SWFPCounter> {
        private String key;
        private int frequent;
        private int dFrequent;
        private int preFrequent;

        public String getKey() {
            return key;
        }

        public SWFPCounter(String key) {
            this.key = key;
            this.frequent = 1;
            this.dFrequent = 0;
            this.preFrequent = 0;
        }

        public void add () {
            frequent += 1;
        }

        public void del() {
            frequent -= 1;
            dFrequent += 1;
        }

        public void refresh() {
            dFrequent = 0;
            preFrequent = preFrequent /2 + frequent/2;
            frequent = 0;
        }

        public int getCount() {
            return frequent + dFrequent + preFrequent;
        }

        /**
         * @return the really visit times in this period.
         */
        public int getReallyCount() {
            return frequent + dFrequent;
        }

        public int compareTo(SWFPCounter o) {
            return o.getCount() - getCount();
        }
    }
}
