package com.sdp.hotspotdetect.bloom;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.bloom.hash.HashFunction;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

public class MultiBloomCounterDetectorImp implements FrequentDetectorInterface {

	public HashFunction hashFunction;
	private List<Integer[]> bloomCounterList;

	/**
	 * The number of bloom filters and the length of single bloom filter.
	 */
	private int bloomFilterNumber = 1;
	private int bloomFilterLength = 10;
	private int frequentThreshold = 50;
	private static int INTERVAL = 30;
	
	public int itemSum = 0;
	public int interval = 0;
	public int bloomRecord = 0;
	public int bloomDecay = 0;
	
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
		bloomFilterNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.MULTI_BLOOM_FILTER_NUMBER);
		bloomFilterLength = (Integer) ConfigManager.propertiesMap.get(ConfigManager.BLOOM_FILTER_LENGTH);
		frequentThreshold = (Integer) ConfigManager.propertiesMap.get(ConfigManager.THRESHOLD);
		INTERVAL = (Integer) ConfigManager.propertiesMap.get(ConfigManager.INTERVAL);

		Log.log.info("[Multi-Bloom Counter] bloomFilterNumber = " + bloomFilterNumber
                + ", bloomFilterLength = " + bloomFilterLength
                + ", frequentThreshold = " + frequentThreshold);
	}

	public boolean registerItem(String key, int preSum) {
		itemSum++;

		interval++;
		if(interval == INTERVAL) {
			interval = 0;
			bloomReset();
		}
		
		if(preSum == 0) {
			setBloom(key);
			return true;
		} else {
			setBloom(key);
			int bloomNumber = findBloomNumber(key);
			if(bloomNumber >= frequentThreshold || bloomNumber == bloomFilterNumber) {
				return true;
			}
			return false;
		}
	}

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

	public void bloomReset() {
		for(int i = 0; i < bloomFilterLength; i++) {
			bloomCounterList.get(bloomDecay)[i] = 0;
		}
		bloomDecay ++;
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
