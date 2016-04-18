package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;

import java.util.Vector;

/**
 * Created by magq on 16/1/12.
 */
public class MultiBloomDetectorImp implements BaseBloomDetector {

    private HashFunction hashFunction;
    private Vector<BloomCounter>[] bloomCounterVector;

    private static int BLOOM_FILTER_NUMBER = 1;
    private static int BLOOM_FILTER_LENGTH = 10;
    private static int HOTSPOT_THRESHOLD = 100;

    public int itemSum = 0;
    public int itemPass = 0;
    
    public MultiBloomDetectorImp(){
        initConfig();
        hashFunction = new HashFunction();
    	bloomCounterVector = new Vector[BLOOM_FILTER_NUMBER];
        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            bloomCounterVector[i] = new Vector<BloomCounter>();
            for (int j = 0; j < BLOOM_FILTER_LENGTH; j++) {
                bloomCounterVector[i].add(new BloomCounter());
            }
        }
    }

    public void initConfig() {
        BLOOM_FILTER_NUMBER = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.MULTI_BLOOM_FILTER_NUMBER);
        BLOOM_FILTER_LENGTH = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.BLOOM_FILTER_LENGTH);
        HOTSPOT_THRESHOLD = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOTSPOT_THRESHOLD);
    }

    public int[] getHashIndex(String key) {
        return hashFunction.getHashIndex(key);
    }

    public boolean registerItem(String key) {
        itemSum++;

        boolean isHotspot = true;
        int[] indexs = getHashIndex(key);
        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            int index = indexs[i];
            if (bloomCounterVector[i].get(index).visit() < HOTSPOT_THRESHOLD) {
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
}
