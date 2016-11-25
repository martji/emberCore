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
     * 1.	寻找频率超过总数1/k的item。
       2.	需要存储k-1对item和counter。
       3.	将新读到的item与已经存入的item比较，如果找到匹配的item，则将相应的counter加一；要是没有一个现存item与其匹配，如果有现存item对应的counter值为0，则将新的item即当前读到的item去替换这个item值，并且counter置为1，要是没有现存item的counter的值为0，则将k-1个counter都减1.
       4.	判定条件：所有计数器监管的item都为热点数据。这样最后存在items中的item肯定包括频率超过1/k的item，但是也存在频率小于1/k的item。必要不充分
     */
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

    /**
     * 原算法中没有
     * @return
     */
    /*public String updateFrequent() {
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
        
    }*/

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
