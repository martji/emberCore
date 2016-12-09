package com.sdp.hotspotdetect.bloom;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.bloom.hash.HashFunction;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MultiBloomCounterDetectorImp implements FrequentDetectorInterface {

    /**
     * The number of bloom filters and the length of single bloom filter.
     */
    private int bloomFilterNumber;
    private int bloomFilterLength;
    private int frequentThreshold;

    /**
     * INTERVAL:当到来INTERVAL个数据，调度擦除一个布隆计数器
     */
    private static int INTERVAL = 30;

    public int itemSum = 0;
    public int interval = 0;
    public int bloomRecord = 0;
    public int bloomDecay = 0;

    public HashFunction hashFunction;
    private List<Integer[]> bloomCounterList;

    public MultiBloomCounterDetectorImp() {
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
        frequentThreshold = (Integer) ConfigManager.propertiesMap.get(ConfigManager.FREQUENT_THRESHOLD);
        INTERVAL = (Integer) ConfigManager.propertiesMap.get(ConfigManager.INTERVAL);

        Log.log.info("[Multi-Bloom Counter] bloomFilterNumber = " + bloomFilterNumber
                + ", bloomFilterLength = " + bloomFilterLength
                + ", frequentThreshold = " + frequentThreshold
                + ", INTERVAL = " + INTERVAL);
    }

    /**
     * 多重布隆过滤器
     * 1.采用V个独立的布隆计数器
     * 2.进行一次数据访问时，用K个哈希函数算出K个哈希值，并将选中的布隆计数器的相应位全设为1，当下一次访问时，通过轮叫调度选择一个布隆计数器记录。
     * 3.更新操作：为了保证实时性，需要定期地选择布隆计数器进行擦除。
     * 4.判断操作：需要寻找一共有几个布隆计数器记录了相应的值来反应其频数。
     * 5.为了能够实现精确的频率统计，当选择记录数据信息的布隆计数器时，如果该计数器已经记录了相同的值，则寻找下一个与要记录的值不同的布隆计数器。
     *
     * @param key
     * @return
     */
    public boolean registerItem(String key) {
        itemSum++;

        interval++;
        if (interval == INTERVAL) {
            interval = 0;
            bloomReset();
        }
        setBloom(key);
        int bloomNumber = findBloomNumber(key);
        if (bloomNumber >= frequentThreshold || bloomNumber == bloomFilterNumber) {
            if (!currentHotSpotCounters.containsKey(key)) {
                currentHotSpotCounters.put(key, 0);
            }
            currentHotSpotCounters.put(key, currentHotSpotCounters.get(key) + 1);
            return true;
        } else {
            return false;
        }
    }

    /**
     * 擦除布隆计数器信息
     */
    public void bloomReset() {
        if (bloomDecay == bloomFilterNumber) {
            bloomDecay = 0;
        }
        for (int i = 0; i < bloomFilterLength; i++) {
            bloomCounterList.get(bloomDecay)[i] = 0;
        }
        bloomDecay++;
    }

    /**
     * 寻找一个合适的布隆计数器记录新到来的key
     *
     * @param key
     * @return
     */
    public boolean setBloom(String key) {
        int[] indexArray = hashFunction.getHashIndex(key);

        for (int i = bloomRecord; i < bloomFilterNumber; i++) {
            for (int j = 0; j < indexArray.length; j++) {
                if (bloomCounterList.get(i)[indexArray[j]] == 0) {
                    for (int k = 0; k < indexArray.length; k++) {
                        bloomCounterList.get(i)[indexArray[k]] = 1;
                        bloomRecord = i + 1;
                        return true;
                    }
                }
            }
        }

        for (int i = 0; i < bloomRecord; i++) {
            for (int j = 0; j < indexArray.length; j++) {
                if (bloomCounterList.get(i)[indexArray[j]] == 0) {
                    for (int k = 0; k < indexArray.length; k++) {
                        bloomCounterList.get(i)[indexArray[k]] = 1;
                        bloomRecord = i + 1;
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * 找有多少个布隆计数器记录了该key
     *
     * @param key
     * @return
     */
    public int findBloomNumber(String key) {
        int number = 0;
        int[] indexArray = hashFunction.getHashIndex(key);
        for (int i = 0; i < bloomFilterNumber; i++) {
            for (int j = 0; j < indexArray.length; j++) {
                if (bloomCounterList.get(i)[indexArray[j]] == 0) {
                    break;
                }
                if (j == indexArray.length - 1) {
                    number++;
                }
            }
        }
        return number;
    }

    public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
        return currentHotSpotCounters;
    }

    public void resetCounter() {
        currentHotSpotCounters.clear();
    }

    public void updateThreshold() {
    }

}
