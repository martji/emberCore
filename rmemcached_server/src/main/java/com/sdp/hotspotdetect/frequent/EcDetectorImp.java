package com.sdp.hotspotdetect.frequent;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class EcDetectorImp extends Thread implements FrequentDetectorInterface {

    private static double frequentPercentage =  0.0001;
	static int counterNumber = 10000;
	private static double errorRate = 0.01;
	
	public int itemSum = 0;

	public ConcurrentHashMap<String, Integer> keyCounters = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, Integer> keyPreCounters = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, Integer> keySumCounters = new ConcurrentHashMap<String, Integer>();
    
    public EcDetectorImp() {
        initConfig();
    }
    
    public void initConfig() {
		frequentPercentage =  (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);
    	errorRate = (Double) ConfigManager.propertiesMap.get(ConfigManager.ERROR_RATE);
//    	counterNumber = (int) (1/errorRate);
	}

	public boolean registerItem(String key, int preSum) {
		itemSum ++;

		if (keyCounters.containsKey(key)) {
			keyCounters.put(key, keyCounters.get(key) + 1);
		} else if (keyCounters.size() < counterNumber) {
			keyCounters.put(key, 1);
			keyPreCounters.put(key, 0);
			keySumCounters.put(key, itemSum);
		} else {
			while (!keyCounters.containsValue(0)) {
				Iterator iter = keyCounters.keySet().iterator();
				String str = null;
				while (iter.hasNext()) {
					str = (String) iter.next();
					keyCounters.put(str, keyCounters.get(str) - 1);
					keyPreCounters.put(str, keyPreCounters.get(str) + 1);
				}
			}
			Iterator iter = keyCounters.keySet().iterator();
			String str = null;
			while (iter.hasNext()) {
				str = (String) iter.next();
				if (keyCounters.get(str) == 0) {
					keyCounters.remove(str);
					keyPreCounters.remove(str);
					keySumCounters.remove(str);
				}
			}
			keyCounters.put(key, 1);
			keyPreCounters.put(key, 0);
			keySumCounters.put(key, itemSum);
		}
		if (keyCounters.get(key) + keyPreCounters.get(key) > (frequentPercentage - errorRate) * itemSum) {	//itemSum是一个变量
			currentHotSpotCounters.put(key, keyCounters.get(key) + keyPreCounters.get(key));
			return true;
		}
        return false;
	}

	public void updateItemsum(int preSum) {
		itemSum -= preSum;
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

	public void refreshSWFPCounter() {

	}
}
