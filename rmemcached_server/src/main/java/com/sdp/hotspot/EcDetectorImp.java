package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class EcDetectorImp extends Thread implements BaseFrequentDetector {

    private static double frequentPercentage =  0.0001;
	
	static int counterNumber = 10000;
	
	private static double errorRate = 0.01;
	
	public int itemSum = 0;

    private ConcurrentHashMap<String, Integer> itemPreCounters = new ConcurrentHashMap<String, Integer>();
	
    private ConcurrentHashMap<String, Integer> itemSumCounters = new ConcurrentHashMap<String, Integer>();

    public ConcurrentHashMap<String, Integer> keyCounters = new ConcurrentHashMap<String, Integer>();
    
    public EcDetectorImp() {
        initConfig();
    }
    
    public void initConfig() {
		// TODO Auto-generated method stub
		frequentPercentage =  (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.FREQUENT_PERCENTAGE);
    	errorRate = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.ERROR_RATE);
//    	counterNumber = (int) (1/errorRate);
	}

	public boolean registerItem(String key) {
		// TODO Auto-generated method stub
		itemSum ++;

		if (keyCounters.containsKey(key)) {
			keyCounters.put(key, keyCounters.get(key) + 1);
		} else if (keyCounters.size() < counterNumber) {
			keyCounters.put(key, 1);
			itemPreCounters.put(key, 0);
			itemSumCounters.put(key, itemSum);
		} else {
			while (!keyCounters.containsValue(0)) {
				Iterator iter = keyCounters.keySet().iterator();
				String str = null;
				while (iter.hasNext()) {
					str = (String) iter.next();
					keyCounters.put(str, keyCounters.get(str) - 1);
					itemPreCounters.put(str, itemPreCounters.get(str) + 1);
				}
			}
			Iterator iter = keyCounters.keySet().iterator();
			String str = null;
			while (iter.hasNext()) {
				str = (String) iter.next();
				if (keyCounters.get(str) == 0) {
					keyCounters.remove(str);
					itemPreCounters.remove(str);
					itemSumCounters.remove(str);
				}
			}
			keyCounters.put(key, 1);
			itemPreCounters.put(key, 0);
			itemSumCounters.put(key, itemSum);
		}
		if (keyCounters.get(key) + itemPreCounters.get(key) > (frequentPercentage - errorRate) * itemSum) {
			itemCounters.put(key, keyCounters.get(key) + itemPreCounters.get(key));
			return true;
		} else {
			return false;
		}
	}

	public void updateItemsum(int preSum) {
		itemSum -= preSum;
	}

	public ConcurrentHashMap<String, Integer> getItemCounters() {
		return itemCounters;
	}

	public void resetCounter() {
		// TODO Auto-generated method stub
		itemCounters.clear();
	}
}
