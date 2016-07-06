package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;

/**
 * Created by magq on 16/1/12.
 */
public class MultiBloomDetectorImp implements BaseBloomDetector {

    private HashFunction hashFunction;
    private List<Integer[]> bloomCounterList;

    /**
     * The number of bloom filters.
     */
    private static int BLOOM_FILTER_NUMBER = 1;
    /**
     * The length of single bloom filter.
     */
    private static int BLOOM_FILTER_LENGTH = 10;

    private static double FREQUENT_PERCENT = 0.8;


    /**
     * The threshold of frequent item, the default value is 2, which means if a item
     * is visited more than once, the item is thought as a frequent item.
     * This parameter is changed to let 20% (1- FREQUENT_PERCENT) items through.
     */
    private int frequent_threshold = 2;
    private int pre_frequent_threshold = 2;

    public int itemSum = 0;
    public int itemPass = 0;

    public MultiBloomDetectorImp(){
        initConfig();
        hashFunction = new HashFunction();
    	bloomCounterList = new ArrayList<Integer[]>();

        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            bloomCounterList.add(new Integer[BLOOM_FILTER_LENGTH]);
            for (int j = 0; j < BLOOM_FILTER_LENGTH; j++) {
                bloomCounterList.get(i)[j] = 0;
            }
        }
    }

    public void initConfig() {
        BLOOM_FILTER_NUMBER = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.MULTI_BLOOM_FILTER_NUMBER);
        BLOOM_FILTER_LENGTH = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.BLOOM_FILTER_LENGTH);
        FREQUENT_PERCENT = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.FREQUENT_PERCENT);
        System.out.println("[bloom_filter_number]: " + BLOOM_FILTER_NUMBER + "; " +
                "[bloom_filter_length]: " + BLOOM_FILTER_LENGTH + "; " +
                "[frequent_percent]: " + FREQUENT_PERCENT);
    }

    public int[] getHashIndex(String key) {
        return hashFunction.getHashIndex(key);
    }

    /**
     *
     * @param key
     * @return whether the item key can through bloom filter.
     */
    public boolean registerItem(String key) {
        itemSum++;

        boolean isHotSpotCandidate = true;
        int[] indexArray = hashFunction.getHashIndex(key);
        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            int index = indexArray[i];
            bloomCounterList.get(i)[index] += 1;
            if (bloomCounterList.get(i)[index] < frequent_threshold) {
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
     * This method is callback each period, adjust the frequent_threshold to
     * ensure the frequent items through.
     */
    public void updateItemSum() {
        if (itemSum > 0) {
            double frequent = (double) itemPass / itemSum;
            if (frequent > FREQUENT_PERCENT) {
                if (pre_frequent_threshold != 2) {
                    frequent_threshold *= 2;
                } else {
                    frequent_threshold += 2;
                }
                pre_frequent_threshold = frequent_threshold;
            } else if (frequent < (FREQUENT_PERCENT / 2)) {
                frequent_threshold = (frequent_threshold + pre_frequent_threshold) / 2;
                frequent_threshold = (frequent_threshold > 2) ? frequent_threshold : 2;

                pre_frequent_threshold = 2;
            }
        } else {
            frequent_threshold = 2;
        }

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(new Date()) + ": [bloom visit count]: " + itemPass +" / "+ itemSum +
                " [frequent_threshold]: " + frequent_threshold);

        itemSum = 0;
        itemPass = 0;
    }

    /**
     * Reset the counter of bloom filter each period.
     */
    public void resetCounter() {
        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            for (int j = 0; j < BLOOM_FILTER_LENGTH; j++) {
                bloomCounterList.get(i)[j] = 0;
            }
        }
    }
}
