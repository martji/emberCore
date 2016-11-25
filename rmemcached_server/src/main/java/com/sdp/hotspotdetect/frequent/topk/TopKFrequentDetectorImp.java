package com.sdp.hotspotdetect.frequent.topk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

public class TopKFrequentDetectorImp implements FrequentDetectorInterface {

	private int counterNumber;
	private double hotSpotInfluence;
	private double hotSpotPercentage;
	private double INIT_HOT_SPOT_PERCENTAGE;

	private ConcurrentHashMap<String, Integer> preValue = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, Integer> CounterMap = new ConcurrentHashMap<String, Integer>();

	public int itemSum = 0;
	private int preItemSum = 0;

	public TopKFrequentDetectorImp() {
		initConfig();
	}

	public void initConfig() {
		counterNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.COUNTER_NUMBER);
		hotSpotPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);
		hotSpotInfluence = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_INFLUENCE);
        INIT_HOT_SPOT_PERCENTAGE = hotSpotPercentage;

        Log.log.info("[TopK] " + "counterNumber = " + counterNumber
                + ", hotSpotPercentage = " + hotSpotPercentage
                + ", hotSpotInfluence = " + hotSpotInfluence);
	}

	/**
	 * 这个和TopKDetectorImp的区别是：那篇论文里面有两种判定热点数据的方法。
     TopKFrequentDetectorImp是根据频率来判断的，TopKDetectorImp是选前K’个数据.
     这个java文件根据频率来判断热点数据，算法伪代码的图在整理文件里。这里和他的原算法有一点不一样，我是用(CounterMap.get(key) - preValue.get(key) >= hotSpotPercentage * itemSum)这个判断，原算法是CounterMap.get(key) >= hotSpotPercentage * itemSum
	 原算法没有对itemSum的更新，所以我也没有更新。
     * @param key
	 * @param preSum
	 * @return
	 */
	public boolean registerItem(String key, int preSum) {
		itemSum++;
		boolean result = false;
		if (CounterMap.containsKey(key)) {
			CounterMap.put(key, CounterMap.get(key) + 1);
			if (CounterMap.get(key) - preValue.get(key) >= hotSpotPercentage * itemSum) {
				currentHotSpotCounters.put(key, CounterMap.get(key) - preValue.get(key));
				result = true;
			}
		} else if (CounterMap.size() < counterNumber) {
			CounterMap.put(key, 1);
			preValue.put(key, 0);
		} else {
			int min = Integer.MAX_VALUE;
			String strMin = null;
			String str;
			Iterator iterator = CounterMap.keySet().iterator();
			while (iterator.hasNext()) {
				str = (String) iterator.next();
				if (CounterMap.get(str) < min) {
					strMin = str;
					min = CounterMap.get(strMin);
				}
			}
			CounterMap.remove(strMin);
			preValue.remove(strMin);
			CounterMap.put(key, min + 1);
			preValue.put(key, min);
		}
		return result;
	}

    /**
     * 这个函数是更新热点频率的，原来的算法中没有
     * @return
     */
	/*public String updateFrequentCounter() {
		itemSum -= preItemSum;
		preItemSum = itemSum;

		System.out.println(currentHotSpotCounters);
		ArrayList<Integer> hotSpots = new ArrayList<Integer>(currentHotSpotCounters.values());
		int totalCount = 0;
		for (int i = 0; i < hotSpots.size(); i++) {
			totalCount += hotSpots.get(i);
		}
		double tmp = (double) totalCount / itemSum;
		if (totalCount > 0 && tmp < hotSpotInfluence) {
			hotSpotPercentage /= 2;
		} else if (totalCount == 0) {
			hotSpotPercentage = INIT_HOT_SPOT_PERCENTAGE;
		}

		String result = "[TopK] frequent counter: " + totalCount + "/" + itemSum + " hot spot percentage: "
				+ hotSpotPercentage;
		return result;
	}*/

	public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
		return currentHotSpotCounters;
	}

	public void resetCounter() {
		currentHotSpotCounters.clear();
	}

}
