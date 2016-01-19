package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;

import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by magq on 16/1/12.
 */
public class MultiBloomDetectorImp implements BaseBloomDetector {

    private static MultiBloomDetectorImp ourInstance = null;
    private Vector<BloomCounter>[] bloomCounterVector;
    private ConcurrentHashMap<String, int[]> hashIndexMap = new ConcurrentHashMap<String, int[]>();

    private static int BLOOM_FILTER_NUMBER = 1;
    private static int BLOOM_FILTER_LENGTH = 10;
    private static int HOTSPOT_THRESHOLD = 100;

    public static MultiBloomDetectorImp getInstance() {
        if (ourInstance == null) {
            ourInstance = new MultiBloomDetectorImp();
            ourInstance.bloomCounterVector = new Vector[BLOOM_FILTER_NUMBER];
            for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
                ourInstance.bloomCounterVector[i] = new Vector<BloomCounter>();
                for (int j = 0; j < BLOOM_FILTER_LENGTH; j++) {
                    ourInstance.bloomCounterVector[i].add(new BloomCounter());
                }
            }
            HashFunction.setHashFunctionNumber(BLOOM_FILTER_NUMBER);
        }
        return ourInstance;
    }

    public void initConfig() {
        BLOOM_FILTER_NUMBER = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.MULTI_BLOOM_FILTER_NUMBER);
        BLOOM_FILTER_LENGTH = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.BLOOM_FILTER_LENGTH);
        HOTSPOT_THRESHOLD = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOTSPOT_THRESHOLD);
    }

    public int[] getHashIndex(String key) {
        int[] hashIndexs;
        if (hashIndexMap.containsKey(key)) {
            hashIndexs = hashIndexMap.get(key);
        } else {
            hashIndexs= HashFunction.getInstance().getHashIndex(key);
            hashIndexMap.put(key, hashIndexs);
        }
        return hashIndexs;
    }

    public boolean registerItem(String key) {
        boolean isHotspot = true;
        int[] indexs = getHashIndex(key);
        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            int index = indexs[i];
            if (bloomCounterVector[i].get(index).visit() < HOTSPOT_THRESHOLD) {
                isHotspot = false;
            }
        }
        return isHotspot;
    }

    public void resetBloomCounters() {

    }

    public void resetBloomCounter(String key) {
        int[] indexs = getHashIndex(key);
        int minCounter = bloomCounterVector[0].get(indexs[0]).getVisitCounter();
        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            int index = indexs[i];
            int tmp = bloomCounterVector[i].get(index).getVisitCounter();
            minCounter = tmp < minCounter ? tmp : minCounter;
        }
        for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
            int index = indexs[i];
            bloomCounterVector[i].get(index).resetVisitCounter(minCounter);
        }
    }

    public Vector<String> getHotSpots() {
        return null;
    }
}
