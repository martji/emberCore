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

    private int hotSpotThreshold = MIN_F;

    /**
     * The really counter to count the visit times of item.
     */
    private ConcurrentHashMap<String, SWFPCounter> counterMap;
    private int counterNumber;

    private int itemSum = 0;
    private int preItemSum = 0;
    private int requestNum = 0;

    public void setRequestNum(int requestNum) {
        this.requestNum = requestNum;
    }

    public SWFPDetectorImp() {
        initConfig();

        counterMap = new ConcurrentHashMap<String, SWFPCounter>();
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
            if (counterMap.size() >= counterNumber) {
                Set<String> keySet = new HashSet<>(counterMap.keySet());
                for (String item : keySet) {
                    if (counterMap.containsKey(item)) {
                        SWFPCounter counter = counterMap.get(item);
                        if (counter != null) {
                            counter.del();
                            if (counter.frequent <= 0) {
                                counterMap.remove(item);
                            }
                        } else {
                            counterMap.remove(item);
                        }
                    }
                }
            }
            if (counterMap.size() < counterNumber) {
                counterMap.put(key, new SWFPCounter(key));
                count = 1;
            }
        }

        if (count > hotSpotThreshold) {
            currentHotSpotCounters.put(key, count);
            return true;
        }
        return false;
    }

    /**
     * Adjust hotSpotPercentage, the workload influence of hot spot must larger
     * than hotSpotInfluence.
     */
    public void updateThreshold() {
        itemSum -= preItemSum;
        preItemSum = itemSum;

        ArrayList<Integer> hotSpots = new ArrayList<Integer>(currentHotSpotCounters.values());
        int totalCount = 0;
        for (int i = 0; i < hotSpots.size(); i++) {
            totalCount += hotSpots.get(i);
        }
        double tmp = (double) totalCount / requestNum;
        if (totalCount > 0) {
            if (tmp < hotSpotInfluence) {
                hotSpotThreshold *= tmp / hotSpotInfluence;
            } else if (tmp > hotSpotInfluence * 3 / 2) {
                hotSpotThreshold *=  hotSpotInfluence / tmp;
            }
            hotSpotThreshold = Math.max(hotSpotThreshold, MIN_F);
        }
        if (itemSum > 0) {
            Log.log.debug("[Threshold] hotSpotInfluence = " + totalCount + "/" + requestNum +
                    ", hotSpotThreshold = " + hotSpotThreshold);
        }
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
