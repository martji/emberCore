package com.sdp.manager.hotspotmanager;

import com.sdp.hotspotdetect.frequent.FrequentDetectorImp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MultiBloomHotSpotManager implement {@link BaseHotSpotManager} and the inner hot spot detector is
 * {@link FrequentDetectorImp}
 */

public class FrequentHotSpotManager extends BaseHotSpotManager {

    private FrequentDetectorImp frequentDetector;

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
        super.handleRegister(key);

        if (frequentDetector != null) {
            if ((frequentDetector.registerItem(key)) && (!currentHotSpotSet.contains(key))) {
                currentHotSpotSet.add(key);
                dealHotData(key);
            }
        }
    }

    @Override
    public void resetCounter() {
        super.resetCounter();
    }

    @Override
    public void recordHotSpot() {
        super.recordHotSpot();

        final List<HotSpotItem> list = new ArrayList<HotSpotItem>();
        Map<String, Integer> map = frequentDetector.getCurrentHotSpot();
        for (String key : map.keySet()) {
            list.add(new HotSpotItem(key, map.get(key)));
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

    public void dealColdData() {
        onFindHotSpot.dealColdSpot();
    }

    public void dealHotData(String key) {
        onFindHotSpot.dealHotSpot(key);
    }

}
