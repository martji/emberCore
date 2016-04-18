package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Created by magq on 16/1/12.
 */
public class MultiBloomDetectorImp implements BaseBloomDetector {

    private HashFunction hashFunction;
    private List<Integer[]> bloomCounterList;

    private static int BLOOM_FILTER_NUMBER = 1;
    private static int BLOOM_FILTER_LENGTH = 10;
    private static int HOTSPOT_THRESHOLD = 100;

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
        HOTSPOT_THRESHOLD = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOTSPOT_THRESHOLD);
        System.out.println("bloom_filter_number: " + BLOOM_FILTER_NUMBER + "; bloom_filter_length: " + BLOOM_FILTER_LENGTH + "; hotspot_threshold: " + HOTSPOT_THRESHOLD);
    }

    public int[] getHashIndex(String key) {
        return hashFunction.getHashIndex(key);
    }

    public boolean registerItem(String key) {
        itemSum++;

        boolean isHotspot = true;
        int[] indexs = hashFunction.getHashIndex(key);
        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            int index = indexs[i];
            bloomCounterList.get(i)[index] += 1;
            if (bloomCounterList.get(i)[index] < HOTSPOT_THRESHOLD) {
                isHotspot = false;
            }
        }
        if (isHotspot) {
            itemPass++;
        }
        return isHotspot;
    }

    public void resetBloomCounters() {

    }

    public Vector<String> getHotSpots() {
        return null;
    }

    public void updateItemsum(int preSum) {
        itemSum -= preSum;
        itemPass = 0;
    }

    public void resetCounter() {
        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            for (int j = 0; j < BLOOM_FILTER_LENGTH; j++) {
                bloomCounterList.get(i)[j] = 0;
            }
        }
    }
}
