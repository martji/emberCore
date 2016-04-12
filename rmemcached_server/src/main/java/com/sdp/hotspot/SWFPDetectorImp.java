package com.sdp.hotspot;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Guoqing on 2016/3/29.
 */
public class SWFPDetectorImp implements BaseFrequentDetector {

    private ConcurrentHashMap<String, SWFPCounter> swfpMap = new ConcurrentHashMap<String, SWFPCounter>();

    private int counterSize = 10000;
    private int threshold = 1000;

    public int itemSum = 0;

    public SWFPDetectorImp() {
        initConfig();
    }

    public void initConfig() {

    }

    public boolean registerItem(String key) {
        itemSum ++;

        if (swfpMap.containsKey(key)) {
            swfpMap.get(key).add();
            if (swfpMap.get(key).getCount() > threshold) {
                itemCounters.put(key, swfpMap.get(key).frequent);
            }
        } else {
            if (swfpMap.size() < counterSize) {
                swfpMap.put(key, new SWFPCounter(key));
            } else {
                Set<String> keySet = swfpMap.keySet();
                for (String item: keySet) {
                    swfpMap.get(item).del();

                    if (swfpMap.get(item).frequent <= 0) {
                        swfpMap.remove(item);
                    }
                }

                if (swfpMap.size() < counterSize) {
                    swfpMap.put(key, new SWFPCounter(key));
                }
            }
        }
        if (itemCounters.containsKey(key)) {
            return true;
        }
        return false;
    }

    public void refreshSWFPCounter() {
        Set<String> keySet = swfpMap.keySet();
        for (String item: keySet) {
            swfpMap.get(item).refresh();
        }
    }

    public void updateItemsum(int preSum) {
        itemSum -= preSum;
    }

    public ConcurrentHashMap<String, Integer> getItemCounters() {
        return itemCounters;
    }

    public void resetCounter() {
        itemCounters.clear();
    }

    public void refreshItemCounters() {
        itemCounters.clear();
    }

    static class SWFPCounter implements Comparable<SWFPCounter>{
        private String key;
        private int frequent;
        private int dfrequent;
        private int prefrequent;

        public SWFPCounter(String key) {
            this.key = key;
            this.frequent = 1;
            this.dfrequent = 0;
            this.prefrequent = 0;
        }

        public void add () {
            frequent += 1;
        }

        public void del() {
            frequent -= 1;
            dfrequent += 1;
        }

        public void refresh() {
            dfrequent = 0;
            prefrequent = prefrequent/2 + frequent/2;
            frequent = frequent/2;
        }

        public int getCount() {
            return frequent + dfrequent + prefrequent;
        }

        public int compareTo(SWFPCounter o) {
            return o.getCount() - getCount();
        }
    }
}
