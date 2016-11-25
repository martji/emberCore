package com.sdp.manager.hotspotmanager;

import com.sdp.hotspotdetect.frequent.FrequentDetectorImp;
import com.sdp.log.Log;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.replicas.LocalSpots;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class FrequentHotSpotManager extends BaseHotSpotManager implements DealHotSpotInterface {

    private FrequentDetectorImp frequentDetector;
	private HashSet<String> currentHotSpotSet = new HashSet<String>();

	public FrequentHotSpotManager() {
		initConfig();

		frequentDetector = new FrequentDetectorImp();
	}

	@Override
	public void initConfig() {
		super.initConfig();
	}

	@Override
	public void handleRegister(String key) {
		if (frequentDetector != null) {
			if ((frequentDetector.registerItem(key, 0)) && (!currentHotSpotSet.contains(key))) {
				currentHotSpotSet.add(key);
				dealHotData(key);
			}
		}
	}

    @Override
    public void resetCounter() {
        /*String frequentCounterOut = frequentDetector.updateFrequent();
        Log.log.info("[Frequent] " + frequentCounterOut);*/
    }

    @Override
	public void recordHotSpot() {
		final List<HotSpotItem> list = new ArrayList<HotSpotItem>();
		Map<String, Integer> map = frequentDetector.getCurrentHotSpot();
		for (String key : map.keySet()) {
			list.add(new HotSpotItem(key, map.get(key)));
		}
		recordCurrentHotSpot(list);
	}

    @Override
    public void dealData() {
        dealHotData();
        LocalSpots.hotSpotNumber.set(currentHotSpotSet.size());
        currentHotSpotSet.clear();
        dealColdData();
    }

    public void dealHotData() {
		onFindHotSpot.dealHotSpot();
	}

	public void dealColdData() {
		onFindHotSpot.dealColdSpot();
	}

	public void dealHotData(String key) {
		onFindHotSpot.dealHotSpot(key);
	}

}
