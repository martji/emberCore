package com.sdp.hotspotdetect.frequent;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.log.Log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by magq on 16/1/12.
 */
public class FrequentDetectorImp implements FrequentDetectorInterface {

    private int frequentItemsNumber = 1;
    private double hotSpotPercentage = 0.0001;
    private static double hotSpotInfluence = 0.1;

    private int itemSum = 0;
    private int preItemSum = 0;

    public FrequentDetectorImp() {
        initConfig();
    }

    public void initConfig() {
    	hotSpotPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);
        hotSpotInfluence = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_INFLUENCE);
        frequentItemsNumber = (int) (1 / hotSpotPercentage);

        Log.log.info("[Frequent] " + "hotSpotPercentage = " + hotSpotPercentage +
                ", hotSpotInfluence = " + hotSpotInfluence);
    }

    /**
     * Frequent algorithm
     */
    public boolean registerItem(String key, int preSum) {
    	itemSum ++;
        boolean result = false;
        if(currentHotSpotCounters.containsKey(key)) {
            currentHotSpotCounters.put(key, currentHotSpotCounters.get(key) + 1);
            result = true;
        } else if(currentHotSpotCounters.size() < frequentItemsNumber) {
            currentHotSpotCounters.put(key, 1);
        } else {
            String str;
            Iterator iterator = currentHotSpotCounters.keySet().iterator();
            while(iterator.hasNext()){
                str = (String) iterator.next();
                if (currentHotSpotCounters.get(str) > 1) {
                    currentHotSpotCounters.put(str, currentHotSpotCounters.get(str) - 1);
                } else {
                    currentHotSpotCounters.remove(str);
                }
            }
        }
        return result;
    }
    
    public String updateFrequent() {
    	itemSum -= preItemSum;
        preItemSum = itemSum;
        
    	ArrayList<Integer> hotSpots = new ArrayList<Integer>(currentHotSpotCounters.values());
    	
        int totalCount = 0;
        for (int i = 0; i < hotSpots.size(); i++) {
            totalCount += hotSpots.get(i);
        }
        double tmp = (double) totalCount / itemSum;
        if (totalCount > 0 && tmp < hotSpotInfluence) {
        	hotSpotPercentage /= 2;
        } else if (totalCount == 0) {
        	hotSpotPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);
        }
        
        frequentItemsNumber = (int) (1 / hotSpotPercentage);

        String result = "[Frequent] frequent counter = " + totalCount + "/"+ itemSum +
                " hot spot percentage = " + hotSpotPercentage;
        return result;
        
    }

    public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
        return currentHotSpotCounters;
    }
    
    public void resetCounter() {
		// TODO Auto-generated method stub
	}

    public String updateFrequentCounter() {
        return null;
    }

}
