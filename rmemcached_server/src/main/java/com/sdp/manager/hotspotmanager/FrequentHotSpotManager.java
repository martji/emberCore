package com.sdp.manager.hotspotmanager;

import com.sdp.hotspotdetect.frequent.FrequentDetectorImp;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;

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
//				dealHotData(key);
			}
		}
	}

    @Override
    public void resetCounter() {
//        String frequentCounterOut = frequentDetector.updateFrequent();
//        Log.log.info(frequentCounterOut + "\n");
    }

    @Override
	public void write2fileBackground() {
		final List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(
				frequentDetector.getItemCounters().entrySet());
		write2file(list);
	}

    @Override
    public void dealData() {
//        dealHotData();
        currentHotSpotSet.clear();
//        dealColdData();
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
