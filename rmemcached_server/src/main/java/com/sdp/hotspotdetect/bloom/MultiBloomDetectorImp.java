package com.sdp.hotspotdetect.bloom;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.bloom.hash.HashFunction;
import com.sdp.hotspotdetect.interfaces.BloomDetectorInterface;
import com.sdp.log.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Created by magq on 16/1/12.
 */
public class MultiBloomDetectorImp implements BloomDetectorInterface {

    private final int LOW_FREQUENT_THRESHOLD = 2;

    /**
     * The number of bloom filters and the length of single bloom filter.
     */
    private int bloomFilterNumber;
    private int bloomFilterLength;
    private double frequentPercentage;

    /**
     * The hotSpotThreshold of frequent item, the default value is 2, which means if a item
     * is visited more than once, the item is thought as a frequent item.
     * This parameter is changed to let 20% (1- frequentPercentage) items through.
     */
    private int frequentThreshold = LOW_FREQUENT_THRESHOLD;
    private int preFrequentThreshold = LOW_FREQUENT_THRESHOLD;

    public int itemSum = 0;
    public int itemPass = 0;

    private HashFunction hashFunction;
    private List<Integer[]> bloomCounterList;

    public MultiBloomDetectorImp() {
        initConfig();

        hashFunction = new HashFunction();
        bloomCounterList = new ArrayList<Integer[]>();
        for (int i = 0; i < bloomFilterNumber; i++) {
            bloomCounterList.add(new Integer[bloomFilterLength]);
            for (int j = 0; j < bloomFilterLength; j++) {
                bloomCounterList.get(i)[j] = 0;
            }
        }
    }

    public void initConfig() {
        bloomFilterNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.BLOOM_FILTER_NUMBER);
        bloomFilterLength = (Integer) ConfigManager.propertiesMap.get(ConfigManager.BLOOM_FILTER_LENGTH);
        frequentPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.FREQUENT_PERCENTAGE);

        Log.log.info("[Multi-Bloom] bloomFilterNumber = " + bloomFilterNumber
                + ", bloomFilterLength = " + bloomFilterLength
                + ", frequentPercent = " + frequentPercentage);
    }

    public int[] getHashIndex(String key) {
        return hashFunction.getHashIndex(key);
    }

    /**
     * @param key
     * @return whether the item key can through bloom filter.
     */
    public boolean registerItem(String key) {
        itemSum++;

        boolean isHotSpotCandidate = true;
        int[] indexArray = hashFunction.getHashIndex(key);
        for (int i = 0; i < bloomFilterNumber; i++) {
            int index = indexArray[i];
            bloomCounterList.get(i)[index] += 1;
            if (bloomCounterList.get(i)[index] < frequentThreshold) {
                isHotSpotCandidate = false;
            }
        }
        if (isHotSpotCandidate) {
            itemPass++;
        }
        return isHotSpotCandidate;
    }

    public void resetBloomCounters() {
    }

    public Vector<String> getHotSpots() {
        return null;
    }

    /**
     * This method is callback each period, adjust the frequentThreshold to
     * ensure the frequent items through.
     */
    public String updateFilterThreshold() {
        if (itemSum > 0) {
            double currentPercentage = (double) itemPass / itemSum;
            if (currentPercentage > frequentPercentage) {
                if (preFrequentThreshold != LOW_FREQUENT_THRESHOLD) {
                    frequentThreshold *= 2;
                } else {
                    frequentThreshold += 2;
                }
                preFrequentThreshold = frequentThreshold;
            } else if (currentPercentage < (frequentPercentage / 2)) {
                frequentThreshold = (frequentThreshold + preFrequentThreshold) / 2;
                frequentThreshold = Math.max(frequentThreshold, LOW_FREQUENT_THRESHOLD);
                preFrequentThreshold = LOW_FREQUENT_THRESHOLD;
            }
        } else {
            frequentThreshold = LOW_FREQUENT_THRESHOLD;
        }

        String result = "[Multi-Bloom Filter] bloom filter pass percentage = " + itemPass + "/" + itemSum +
                " frequentThreshold = " + frequentThreshold;

        itemSum = 0;
        itemPass = 0;

        return result;
    }

    /**
     * Reset the counter of bloom filter each period.
     */
    public void resetCounter() {
        for (int i = 0; i < bloomFilterNumber; i++) {
            for (int j = 0; j < bloomFilterLength; j++) {
                bloomCounterList.get(i)[j] = 0;
            }
        }
    }

}
