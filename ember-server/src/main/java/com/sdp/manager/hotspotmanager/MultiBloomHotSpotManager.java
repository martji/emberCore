package com.sdp.manager.hotspotmanager;

import com.sdp.hotspotdetect.bloom.MultiBloomCounterDetectorImp;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.replicas.LocalSpots;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * MultiBloomHotSpotManager implement {@link BaseHotSpotManager} and the inner hot spot detector is
 * {@link MultiBloomCounterDetectorImp}
 */

public class MultiBloomHotSpotManager extends BaseHotSpotManager implements DealHotSpotInterface {

    private MultiBloomCounterDetectorImp frequentDetector;
    private HashSet<String> currentHotSpotSet = new HashSet<String>();

    public MultiBloomHotSpotManager() {
        initConfig();

        frequentDetector = new MultiBloomCounterDetectorImp();
    }

    @Override
    public void initConfig() {
        super.initConfig();
    }

    @Override
    public void handleRegister(String key) {
        if (frequentDetector != null) {
            if ((frequentDetector.registerItem(key)) && (!currentHotSpotSet.contains(key))) {
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
        ArrayList<String> li = new ArrayList<String>(currentHotSpotSet);
        for (int i = 0; i < li.size(); i++) {
            String str = li.get(i);
            int num = frequentDetector.findBloomNumber(str);
            frequentDetector.currentHotSpotCounters.put(str, num);
        }
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
