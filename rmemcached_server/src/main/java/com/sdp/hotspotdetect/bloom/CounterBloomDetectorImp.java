package com.sdp.hotspotdetect.bloom;

import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.bloom.hash.HashFunction;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

public class CounterBloomDetectorImp implements FrequentDetectorInterface {

    private int bloomFilterLength;
    private int frequentThreshold;

    @Deprecated
    private int hotSpotNum;

    public HashFunction hashFunction;
    private int[] bloomCounter;
    private int[] bloomPreCounter;

    private int item = 0;

    public CounterBloomDetectorImp() {
        initConfig();

        hashFunction = new HashFunction();
        bloomCounter = new int[bloomFilterLength];
        bloomPreCounter = new int[bloomFilterLength];
        for (int i = 0; i < bloomFilterLength; i++) {
            bloomCounter[i] = 0;
            bloomPreCounter[i] = 0;
        }
    }

    public void initConfig() {
        bloomFilterLength = (Integer) ConfigManager.propertiesMap.get(ConfigManager.BLOOM_FILTER_LENGTH);
        frequentThreshold = (Integer) ConfigManager.propertiesMap.get(ConfigManager.FREQUENT_THRESHOLD);
        hotSpotNum = (Integer) ConfigManager.propertiesMap.get(ConfigManager.COUNTER_NUMBER);

        Log.log.info("[Counter Bloom] bloomFilterLength = " + bloomFilterLength
                + ", frequentThreshold = " + frequentThreshold);
    }

    /**
     * 只使用了一个布隆过滤器
     * 当一个地址被访问时，通过哈希函数将其映射到相应位，并将其值加一。
     * 当计数器相应位的值都大于一个阈值时才被判定为热点数据
     *
     * @param key
     * @return
     */
    public boolean registerItem(String key) {
        item++;
        boolean isHotSpot = true;
        int[] indexArray = hashFunction.getHashIndex(key);
        for (int i = 0; i < indexArray.length; i++) {
            int index = indexArray[i];
            bloomCounter[index] += 1;
            if (bloomCounter[index] < frequentThreshold) {
                isHotSpot = false;
            }
        }
        if (isHotSpot) {
            if (!currentHotSpotCounters.containsKey(key)) {
                currentHotSpotCounters.put(key, 0);
            }
            currentHotSpotCounters.put(key, currentHotSpotCounters.get(key) + 1);
        }
        return isHotSpot;
    }

    /**
     * 原算法中是按计数器移位方式来过滤之前的数据
     * 我写的是是保留之前一段时间间隔的数据对热点数据判定的影响
     */
    public void updateCounter() {
        for (int i = 0; i < bloomFilterLength; i++) {
            bloomCounter[i] -= bloomPreCounter[i];
            bloomPreCounter[i] = bloomCounter[i];
        }
    }

    /**
     * @deprecated 原算法中没有
     */
    public void updateThreshold(int number) {
        double percent = (double) number / hotSpotNum;
        if (item > 500) {
            if (percent < 0.7) {
                if (frequentThreshold * 0.7 > 15) {
                    frequentThreshold = (int) (frequentThreshold * 0.7);
                } else {
                    frequentThreshold = 15;
                }
            } else if (percent > 1.5) {
                frequentThreshold = (int) (frequentThreshold * (percent + 2)) / 3;
            }
        }
        item = 0;
        Log.log.info("[Counter Bloom] update frequentThreshold = " + frequentThreshold);
    }

    public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
        return currentHotSpotCounters;
    }

    public void resetCounter() {
        currentHotSpotCounters.clear();
    }

    public String updateHotSpot() {
        updateCounter();
        return null;
    }

}
