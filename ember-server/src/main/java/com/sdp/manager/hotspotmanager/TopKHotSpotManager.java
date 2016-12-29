package com.sdp.manager.hotspotmanager;

import com.sdp.hotspotdetect.frequent.topk.TopKDetectorImp;
import com.sdp.hotspotdetect.frequent.topk.TopKFrequentDetectorImp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TopKHotSpotManager implement {@link BaseHotSpotManager} and the inner hot spot detector is
 * {@link com.sdp.hotspotdetect.frequent.topk.TopKDetectorImp} and {@link TopKFrequentDetectorImp}
 */

public class TopKHotSpotManager extends BaseHotSpotManager {

    private TopKDetectorImp frequentDetector;

    public TopKHotSpotManager() {
        initConfig();

        frequentDetector = new TopKDetectorImp();
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

        frequentDetector.updateThreshold();
        frequentDetector.resetCounter();
    }

    @Override
    public void recordHotSpot() {
        super.recordHotSpot();

        final List<HotSpotItem> list = new ArrayList<HotSpotItem>();
        Map<String, Integer> map = new HashMap<>(frequentDetector.getCurrentHotSpot());
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
