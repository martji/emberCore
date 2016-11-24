package com.sdp.hotspotdetect.bloom;

import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.bloom.hash.HashFunction;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

public class CounterBloomDetectorImp implements FrequentDetectorInterface {

	private int bloomFilterLength = 1000;
	private int frequentThreshold = 30;
    private int hotSpotNum = 1000;

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
		frequentThreshold = (Integer) ConfigManager.propertiesMap.get(ConfigManager.THRESHOLD);
		hotSpotNum = (Integer) ConfigManager.propertiesMap.get(ConfigManager.COUNTER_NUMBER);

        Log.log.info("[Counter Bloom] bloomFilterLength = " + bloomFilterLength
                + ", frequentThreshold = " + frequentThreshold
                + ", hotSpotNum = " + hotSpotNum);
    }

	public boolean registerItem(String key, int preSum) {
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
		return isHotSpot;
	}

	public void updateCounter() {
		for (int i = 0; i < bloomFilterLength; i++) {
			bloomCounter[i] -= bloomPreCounter[i];
			bloomPreCounter[i] = bloomCounter[i];
		}
	}

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
				frequentThreshold = (int) (frequentThreshold * (percent + 2))/3;
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

	public String updateFrequentCounter() {
		return null;
	}

}
