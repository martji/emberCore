package com.sdp.manager.hotspotmanager;

import com.sdp.hotspotdetect.bloom.CounterBloomDetectorImp;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.replicas.LocalSpots;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CounterBloomHotSpotManager extends BaseHotSpotManager implements DealHotSpotInterface {

	private CounterBloomDetectorImp frequentDetector;
	private Set<String> currentHotSpotSet =  Collections.synchronizedSet(new HashSet<String>());

	public CounterBloomHotSpotManager() {
		initConfig();

		frequentDetector = new CounterBloomDetectorImp();
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
         frequentDetector.resetCounter();
    }

    @Override
	public void recordHotSpot() {

	}

    @Override
    public void dealData() {
		dealHotData();
        frequentDetector.updateThreshold(currentHotSpotSet.size());
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
