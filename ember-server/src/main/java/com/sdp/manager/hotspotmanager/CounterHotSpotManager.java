package com.sdp.manager.hotspotmanager;

import com.sdp.config.ConfigManager;
import com.sdp.log.Log;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author magq
 *         CounterHotSpotManager implement {@link BaseHotSpotManager} and detect the hot spots just by
 *         counting the data items.
 *         <p>
 *         Counter all the data. If the visit time of one item is bigger than the predefined hotSpotThreshold,
 *         this item is considered as a hot spot.
 */

public class CounterHotSpotManager extends BaseHotSpotManager {

    private ConcurrentHashMap<String, Integer> countMap;

    private final int HOT_SPOT_NUMBER = 1000;

    private int hotSpotThreshold;

    private int LOW_HOT_SPOT_THRESHOLD;

    public CounterHotSpotManager() {
        initConfig();

        countMap = new ConcurrentHashMap<String, Integer>();
    }

    @Override
    public void initConfig() {
        super.initConfig();

        hotSpotThreshold = (Integer) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_THRESHOLD);
        LOW_HOT_SPOT_THRESHOLD = hotSpotThreshold;

        Log.log.info("[Counter] " + "hotSpotThreshold = " + hotSpotThreshold);
    }

    @Override
    public void handleRegister(String key) {
        super.handleRegister(key);

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
        super.resetCounter();

        updateThreshold();
        countMap.clear();
    }

    public void updateThreshold() {
        if (currentHotSpotSet.size() > 0) {
            double rate = HOT_SPOT_NUMBER / currentHotSpotSet.size();
            hotSpotThreshold /= rate;
            hotSpotThreshold = Math.max(hotSpotThreshold, LOW_HOT_SPOT_THRESHOLD);
            Log.log.info("[Counter] hotSpotThreshold = " + hotSpotThreshold + ", rate = " + rate);
        }
    }

    @Override
    public void recordHotSpot() {
        super.recordHotSpot();

        final List<HotSpotItem> list = new ArrayList<HotSpotItem>();
        Set<String> keys = new HashSet<>(currentHotSpotSet);
        for (String key : keys) {
            if (countMap.containsKey(key)) {
                list.add(new HotSpotItem(key, countMap.get(key)));
            }
        }
        recordCurrentHotSpot(list);
    }

    @Override
    public void dealData() {
        super.dealData();
    }

    public void dealHotData() {
        onFindHotSpot.dealHotSpot();
    }

    public void dealHotData(String key) {
        onFindHotSpot.dealHotSpot(key);
    }

    public void dealColdData() {
        onFindHotSpot.dealColdSpot();
    }
}
