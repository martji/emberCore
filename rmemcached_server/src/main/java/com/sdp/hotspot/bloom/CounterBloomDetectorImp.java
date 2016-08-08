package com.sdp.hotspot.bloom;

import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.hotspot.hash.HashFunction;
import com.sdp.hotspot.interfaces.BaseFrequentDetector;

public class CounterBloomDetectorImp extends Thread implements BaseFrequentDetector {

	private static int BLOOM_FILTER_LENGTH = 1000;
	public HashFunction hashFunction;
	private int frequent_threshold = 30;
	private int[] bloomCounter;
	private int[] bloomPreCounter;
	private int hotSpotNum = 1000;
	private int item = 0;

	public CounterBloomDetectorImp() {
		initConfig();
		hashFunction = new HashFunction();
		bloomCounter = new int[BLOOM_FILTER_LENGTH];
		bloomPreCounter = new int[BLOOM_FILTER_LENGTH];
		for (int i = 0; i < BLOOM_FILTER_LENGTH; i++) {
			bloomCounter[i] = 0;
			bloomPreCounter[i] = 0;
		}
	}

	public void initConfig() {
		// TODO Auto-generated method stub
		BLOOM_FILTER_LENGTH = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.BLOOM_FILTER_LENGTH);
		frequent_threshold = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.THRESHOLD);
		hotSpotNum = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.COUNTER_NUMBER);

	}

	public boolean registerItem(String key, int presum) {
		// TODO Auto-generated method stub
		item++;
		boolean isHotSpot = true;
		int[] indexArray = hashFunction.getHashIndex(key);
		for (int i = 0; i < indexArray.length; i++) {
			int index = indexArray[i];
			bloomCounter[index] += 1;
			if (bloomCounter[index] < frequent_threshold) {
				isHotSpot = false;
			}
		}
		return isHotSpot;
	}

	public void updateCounter() {
		for (int i = 0; i < BLOOM_FILTER_LENGTH; i++) {
			bloomCounter[i] -= bloomPreCounter[i];
			bloomPreCounter[i] = bloomCounter[i];
		}
	}

	public void updateThreahold(int number) {
		double percent = (double) number / hotSpotNum;
		if (item > 500) {
			if (percent < 0.7) {
				if (frequent_threshold * 0.7 > 15) {
					frequent_threshold = (int) (frequent_threshold * 0.7);
				} else {
					frequent_threshold = 15;
				}
			} else if (percent > 1.5) {
				frequent_threshold = (int) (frequent_threshold * (percent + 2))/3;
			}
		}
		item = 0;
		System.out.println(frequent_threshold + "***");
	}

	public ConcurrentHashMap<String, Integer> getItemCounters() {
		// TODO Auto-generated method stub
		return itemCounters;
	}

	public void resetCounter() {
		// TODO Auto-generated method stub
		itemCounters.clear();

	}

}
