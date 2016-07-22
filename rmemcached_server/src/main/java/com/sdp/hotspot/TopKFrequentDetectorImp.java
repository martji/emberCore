package com.sdp.hotspot;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.hotspot.SWFPDetectorImp.SWFPCounter;

public class TopKFrequentDetectorImp extends Thread implements BaseFrequentDetector {

	private static int counterNumber = 20;
	private static double HOT_SPOT_INFLUENCE = 0.1;
	private double hotSpotPercentage = 0.0001;
	private ConcurrentHashMap<String, Integer> preValue = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, Integer> CounterMap = new ConcurrentHashMap<String, Integer>();
	public int itemSum = 0;
	private int preItemSum = 0;

	public TopKFrequentDetectorImp() {
		initConfig();
	}

	public void initConfig() {
		// TODO Auto-generated method stub
		counterNumber = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.COUNTER_NUMBER);
		hotSpotPercentage = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOT_SPOT_PERCENTAGE);
		HOT_SPOT_INFLUENCE = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOT_SPOT_INFLUENCE);
	}

	public boolean registerItem(String key, int presum) {
		// TODO Auto-generated method stub
		itemSum++;
		boolean result = false;
		if (CounterMap.contains(key)) {
			CounterMap.put(key, CounterMap.get(key) + 1);
			if (CounterMap.get(key) - preValue.get(key) >= hotSpotPercentage * itemSum) {
				itemCounters.put(key, CounterMap.get(key) - preValue.get(key));
				result = true;
			}
		} else if (CounterMap.size() < counterNumber) {
			CounterMap.put(key, 1);
			preValue.put(key, 0);
		} else {
			int min = Integer.MAX_VALUE;
			String strMin = null;
			String str = null;
			Iterator iter = CounterMap.keySet().iterator();
			while (iter.hasNext()) {
				str = (String) iter.next();
				if (CounterMap.get(str) < min) {
					strMin = str;
					min = CounterMap.get(str);
				}
			}
			CounterMap.remove(strMin);
			preValue.remove(strMin);
			CounterMap.put(key, min + 1);
			preValue.put(key, min);
		}
		return result;
	}

	public String updateItemSum() {
		itemSum -= preItemSum;
		preItemSum = itemSum;

		ArrayList<Integer> hotSpots = new ArrayList<Integer>(itemCounters.values());
		int totalCount = 0;
		for (int i = 0; i < hotSpots.size(); i++) {
			totalCount += hotSpots.get(i);
		}
		double tmp = (double) totalCount / itemSum;
		if (totalCount > 0 && tmp < HOT_SPOT_INFLUENCE) {
			hotSpotPercentage /= 2;
		} else if (totalCount == 0) {
			hotSpotPercentage = 0.0001;
		}

		String result = "  |  [frequent counter]: " + totalCount + " / " + itemSum + " [hot_spot_percentage]: "
				+ hotSpotPercentage;
		return result;
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