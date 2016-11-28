package com.sdp.hotspotdetect.frequent;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Guoqing on 2016/3/29.
 */
public class SWFPDetectorImp implements FrequentDetectorInterface {

    /**
     * The hotSpotThreshold of hot spot frequent percentage, which is p; and the influence of hot spots.
     */
    private double hotSpotPercentage;
    private double hotSpotInfluence;
    private int MIN_F = 2;

    /**
     * The really counter to count the visit times of item.
     */
    private ConcurrentHashMap<String, SWFPCounter> counterMap;
    private int counterNumber;
    private HashSet<String> preHotSpotSet;

    public int itemSum = 0;
    private int preItemSum = 0;

    public SWFPDetectorImp() {
        initConfig();

        counterMap = new ConcurrentHashMap<String, SWFPCounter>();
        preHotSpotSet = new HashSet<String>();
    }

    public void initConfig() {
        hotSpotPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);
        hotSpotInfluence = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_INFLUENCE);
        counterNumber = (int) (1 / hotSpotPercentage);

        Log.log.info("[SWFP] " + "hotSpotPercentage = " + hotSpotPercentage +
                ", hotSpotInfluence = " + hotSpotInfluence);
    }

    public boolean registerItem(String key) {
        itemSum++;

        int count = 0;
        if (counterMap.containsKey(key)) {
            counterMap.get(key).add();
            if (counterMap.get(key).frequent >= MIN_F) {
                count = counterMap.get(key).getReallyCount();
            }
        } else {
            if (counterMap.size() < counterNumber) {
                counterMap.put(key, new SWFPCounter(key));
            } else {
                Set<String> keySet = counterMap.keySet();
                for (String item : keySet) {
                    counterMap.get(item).del();
                    if (counterMap.get(item).frequent <= 0) {
                        counterMap.remove(item);
                    }
                }
                if (counterMap.size() < counterNumber) {
                    counterMap.put(key, new SWFPCounter(key));
                }
            }
        }

        if (preHotSpotSet.contains(key)) {
            currentHotSpotCounters.put(key, count);
            return true;
        }
        return false;
    }

    /**
     * Adjust hotSpotPercentage, the workload influence of hot spot must larger
     * than hotSpotInfluence.
     */
    public String updateHotSpot() {
        itemSum -= preItemSum;
        preItemSum = itemSum;

        ArrayList<Integer> hotSpots = new ArrayList<Integer>(currentHotSpotCounters.values());
        int totalCount = 0;
        for (int i = 0; i < hotSpots.size(); i++) {
            totalCount += hotSpots.get(i);
        }
        double tmp = (double) totalCount / itemSum;
        if (totalCount > 0) {
            if (tmp < hotSpotInfluence) {
                counterNumber *= 2;
            } else if (tmp > 2 * hotSpotInfluence) {
                counterNumber /= 2;
            }
        }

        String result = "[SWFP] hot spot influence = " + totalCount + "/" + itemSum +
                " counterNumber = " + counterNumber;
        return result;
    }

    public void resetCounter() {
        currentHotSpotCounters.clear();
        refreshSWFPCounter();
    }

    public void refreshSWFPCounter() {
        Set<String> keySet = counterMap.keySet();
        for (String item : keySet) {
            counterMap.get(item).refresh();
        }
        preHotSpotSet = new HashSet<String>(keySet);
    }

    public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
        return currentHotSpotCounters;
    }

    public class SWFPCounter implements Comparable<SWFPCounter> {
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

        public void add() {
            frequent += 1;
        }

        public void del() {
            frequent -= 1;
            dFrequent += 1;
        }

        public void refresh() {
            dFrequent = 0;
            preFrequent = preFrequent / 2 + frequent / 2;
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
