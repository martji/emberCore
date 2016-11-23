package com.sdp.manager.hotspotmanager;

import com.sdp.config.ConfigManager;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.replicas.LocalSpots;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.*;

/**
 * @author magq
 * CounterHotSpotManager implement {@link BaseHotSpotManager} and detect the hot spots just by
 * counting the data items.
 */

public class CounterHotSpotManager extends BaseHotSpotManager implements DealHotSpotInterface {

    private ConcurrentHashMap<String, Integer> countMap = new ConcurrentHashMap<String, Integer>();
    private TreeMap<String, Integer> currentHotSpotSet;

    private double hotSpotPercentage = 0.0001;
    private int heapSize;

	public CounterHotSpotManager() {
        initConfig();

        currentHotSpotSet = new TreeMap<String, Integer>(new Comparator<String>() {
            public int compare(String str1, String str2) {
                try {
                    return countMap.get(str2) - countMap.get(str1);
                } catch (Exception e) {}
                return 0;
            }
        });
	}

	@Override
    public void initConfig() {
        super.initConfig();

        hotSpotPercentage = (Double) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_PERCENTAGE);
        heapSize = (int) (1 / hotSpotPercentage);
    }

	@Override
	public void handleRegister(String key) {
        LocalSpots.candidateColdSpots.remove(key);
		if (!countMap.containsKey(key)) {
			countMap.put(key, 0);
		}
		int visits = countMap.get(key) + 1;
        countMap.put(key, visits);
		if (visits >= LocalSpots.threshold && !currentHotSpotSet.containsKey(key)) {
            if (currentHotSpotSet.size() >= heapSize) {
                currentHotSpotSet.remove(currentHotSpotSet.lastKey());
            }
			currentHotSpotSet.put(key, visits);
            dealHotData(key);
		}
	}

    @Override
    public void resetCounter() {
        super.resetCounter();

        countMap = new ConcurrentHashMap<String, Integer>();
    }

    @Override
    public void write2fileBackground() {
        final List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>
                (currentHotSpotSet.entrySet());
       write2file(list);
    }

    @Override
    public void dealData() {
        dealHotData();
        LocalSpots.hotSpotNumber.set(currentHotSpotSet.size());
        currentHotSpotSet.clear();
        dealColdData();
    }

    public void dealHotData() {
        heapSize = Math.max((int) (countMap.size() * hotSpotPercentage), LocalSpots.threshold);
        onFindHotSpot.dealHotSpot();
    }

    public void dealHotData(String key) {
        onFindHotSpot.dealHotSpot(key);
    }

	public void dealColdData() {
        onFindHotSpot.dealColdSpot();
	}
}
