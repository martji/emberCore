package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by magq on 16/1/12.
 */
public class FrequentDetectorImp implements BaseFrequentDetector {

    private int frequentItemsNumber = 1;
    private double hotSpotPercentage = 0.0001;
    private static FrequentDetectorImp ourInstance = null;
    private static double HOT_SPOT_INFLUENCE = 0.1;
    private int itemSum = 0;
    private int preItemSum = 0;
    /*public static FrequentDetectorImp getInstance() {
        if (ourInstance == null) {
            ourInstance = new FrequentDetectorImp();
        }
        return ourInstance;
    }*/

    public FrequentDetectorImp() {
        initConfig();
    }

    public void initConfig() {
    	hotSpotPercentage = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOT_SPOT_PERCENTAGE);
        HOT_SPOT_INFLUENCE = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOT_SPOT_INFLUENCE);
        frequentItemsNumber = (int) (1 / hotSpotPercentage);
    }

    /**
     * Frequent algorithm
     */
    public boolean registerItem(String key, int presum) {
    	itemSum ++;
        boolean result = false;
        if(itemCounters.containsKey(key)) {
            itemCounters.put(key, itemCounters.get(key) + 1);
            result = true;
        } else if(itemCounters.size() < frequentItemsNumber) {
            itemCounters.put(key, 1);
        } else {
            String str = null;
            Iterator iter = itemCounters.keySet().iterator();
            while(iter.hasNext()){
                str = (String) iter.next();
                if (itemCounters.get(str) > 1) {
                    itemCounters.put(str, itemCounters.get(str) - 1);
                } else {
                    itemCounters.remove(str);
                }
            }
        }
        return result;
    }
    
    public String updateFrequent() {
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
        	hotSpotPercentage = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.HOT_SPOT_PERCENTAGE);
        }
        
        frequentItemsNumber = (int) (1 / hotSpotPercentage);

        String result =  "  |  [frequent counter]: " + totalCount + " / "+ itemSum +
                " [hot_spot_percentage]: " + hotSpotPercentage;
        return result;
        
    }

    public ConcurrentHashMap<String, Integer> getItemCounters() {
        return itemCounters;
    }
    
    public void resetCounter() {
		// TODO Auto-generated method stub
		itemCounters.clear();
	}
}
