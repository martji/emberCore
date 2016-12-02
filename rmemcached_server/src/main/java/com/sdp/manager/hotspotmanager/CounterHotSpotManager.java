package com.sdp.manager.hotspotmanager;

import com.sdp.config.ConfigManager;
import com.sdp.log.Log;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.replicas.LocalSpots;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @author magq
 *         CounterHotSpotManager implement {@link BaseHotSpotManager} and detect the hot spots just by
 *         counting the data items.
 *         <p>
 *         Counter all the data. If the visit time of one item is bigger than the predefined hotSpotThreshold,
 *         this item is considered as a hot spot.
 */

public class CounterHotSpotManager extends BaseHotSpotManager implements DealHotSpotInterface {

    private ConcurrentHashMap<String, Integer> countMap = new ConcurrentHashMap<String, Integer>();
    private HashSet<String> currentHotSpotSet;

    public int hotSpotThreshold;
    private double hotSpotPercentage;

    private static final int LOW_HOT_SPOT_THRESHOLD = 2;

    public CounterHotSpotManager() {
        initConfig();

        currentHotSpotSet = new HashSet<String>();
    }

    @Override
    public void initConfig() {
        super.initConfig();

        hotSpotThreshold = (Integer) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_THRESHOLD);
        hotSpotPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);

        Log.log.info("[TopK Frequent] " + "hotSpotThreshold = " + hotSpotThreshold
                + ", hotSpotPercentage = " + hotSpotPercentage);
    }

    @Override
    public void handleRegister(String key) {
        LocalSpots.candidateColdSpots.remove(key);
        if (!countMap.containsKey(key)) {
            countMap.put(key, 0);
        }
        int visits = countMap.get(key) + 1;
        countMap.put(key, visits);
        if (visits >= hotSpotThreshold && !currentHotSpotSet.contains(key)) {
            currentHotSpotSet.add(key);
            dealHotData(key);
        }
    }

    @Override
    public void resetCounter() {
        countMap = new ConcurrentHashMap<String, Integer>();
    }

    @Override
    public void recordHotSpot() {
        final List<HotSpotItem> list = new ArrayList<HotSpotItem>();
        for (String key : currentHotSpotSet) {
            if (countMap.containsKey(key)) {
                list.add(new HotSpotItem(key, countMap.get(key)));
            }
        }
        recordCurrentHotSpot(list);
    }

    @Override
    public void dealData() {
        dealHotData();
        LocalSpots.hotSpotNumber.set(currentHotSpotSet.size());
        dealColdData();
    }

    public void dealHotData() {
        hotSpotThreshold /= (countMap.size() * hotSpotPercentage) / currentHotSpotSet.size();
        hotSpotThreshold = Math.max(hotSpotThreshold, LOW_HOT_SPOT_THRESHOLD);
        onFindHotSpot.dealHotSpot();
    }

    public void dealHotData(String key) {
        onFindHotSpot.dealHotSpot(key);
    }

    public void dealColdData() {
        onFindHotSpot.dealColdSpot();
    }
}
