package com.sdp.hotspot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.example.Log;

public class MultiBloomCounterDetectorImp extends Thread implements BaseFrequentDetector {

	public HashFunction hashFunction;
	private List<Integer[]> bloomCounterList;

	/**
	 * The number of bloom filters.
	 */
	private static int BLOOM_FILTER_NUMBER = 1;
	/**
	 * The length of single bloom filter.
	 */
	private static int BLOOM_FILTER_LENGTH = 10;
	private int frequent_threshold = 30;
	private static int INTERVAL = 30;
	
	public int itemSum = 0;
	public int interval = 0;
	public int bloomRecord = 0;
	public int bloomDecay = 0;
	
	public MultiBloomCounterDetectorImp() {
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
		// TODO Auto-generated method stub
		BLOOM_FILTER_NUMBER = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.MULTI_BLOOM_FILTER_NUMBER);
		BLOOM_FILTER_LENGTH = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.BLOOM_FILTER_LENGTH);
		frequent_threshold = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.THRESHOLD);
		INTERVAL = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.INTERVAL);
		Log.log.info("[bloom_filter_number]: " + BLOOM_FILTER_NUMBER + "; " + "[bloom_filter_length]: "
				+ BLOOM_FILTER_LENGTH + "; " + "[frequent_threshold]: " + frequent_threshold);
	}

	public boolean registerItem(String key, int presum) {
		// TODO Auto-generated method stub
		//itemSum++;
		interval++;
		if(interval == INTERVAL) {
			interval = 0;
			bloomReset();
		}
		
		if(presum == 0) {
			setBloom(key);
			return true;
		}else {
			setBloom(key);
			int bloomNumber = findBloomNumber(key);
			if(bloomNumber >= frequent_threshold || bloomNumber == BLOOM_FILTER_NUMBER) {
				return true;
			}
			else {
				return false;
			}
		}
	}

	public boolean setBloom(String key) {
		int[] indexArray = hashFunction.getHashIndex(key);

		for (int i = bloomRecord; i < BLOOM_FILTER_NUMBER; i++) {
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
		for (int i = 0; i < BLOOM_FILTER_NUMBER; i++) {
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
		for(int i = 0;i < BLOOM_FILTER_LENGTH;i++) {
			bloomCounterList.get(bloomDecay)[i] = 0;
		}
		bloomDecay ++;
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
