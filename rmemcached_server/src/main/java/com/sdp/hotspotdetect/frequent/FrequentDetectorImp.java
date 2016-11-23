package com.sdp.hotspot.frequent;

import com.sdp.config.ConfigManager;
import com.sdp.hotspot.interfaces.BaseFrequentDetector;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by magq on 16/1/12.
 */
public class FrequentDetectorImp implements BaseFrequentDetector {

    private int frequentItemsNumber = 1;
    private double hotSpotPercentage = 0.0001;
    private static double HOT_SPOT_INFLUENCE = 0.1;
    private int itemSum = 0;
    private int preItemSum = 0;

    public ConcurrentHashMap<String, Integer> PreitemCounters = new ConcurrentHashMap<String, Integer>();
    
    public FrequentDetectorImp() {
        initConfig();
    }

    public void initConfig() {
    	hotSpotPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);
        HOT_SPOT_INFLUENCE = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_INFLUENCE);
        frequentItemsNumber = (int) (1/hotSpotPercentage);
    }

    /**
     * Frequent algorithm
     */
    public boolean registerItem(String key, int preSum) {
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
    
    /*public String updateFrequent() {
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
        	hotSpotPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);
        }
        
        frequentItemsNumber = (int) (1 / hotSpotPercentage);

        String result =  "  |  [frequent counter]: " + totalCount + " / "+ itemSum +
                " [hot_spot_percentage]: " + hotSpotPercentage;
        return result;
        
    }*/

    public ConcurrentHashMap<String, Integer> getItemCounters() {
        return itemCounters;
    }
    
    public void resetCounter() {
		// TODO Auto-generated method stub
	}

    public String updateItemSum() {
        return null;
    }

    public void refreshSWFPCounter() {

    }
}
