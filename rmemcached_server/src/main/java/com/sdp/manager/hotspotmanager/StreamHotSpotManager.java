package com.sdp.manager.hotspotmanager;

import com.sdp.hotspotdetect.bloom.MultiBloomDetectorImp;
import com.sdp.hotspotdetect.frequent.SWFPDetectorImp;
import com.sdp.hotspotdetect.interfaces.BaseBloomDetector;
import com.sdp.hotspotdetect.interfaces.BaseFrequentDetector;
import com.sdp.log.Log;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.replicas.LocalSpots;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Created by magq on 16/7/6.
 * StreamHotSpotManager implement {@link BaseHotSpotManager} and detects the hot spots by a
 * 3-steps stream mining algorithm. This algorithm contains three steps: data sampling, data filter
 * and data counter.
 *
 * The data sampling is done by the ember client side and the sampling percentage can be configured.
 *
 * The data filter{@link BaseBloomDetector} is based on the 20/80 theory, and the hot spots must be in
 * the 20%.
 *
 * The data counter{@link BaseFrequentDetector} intends to find the hot spots by counting the data items,
 * but it does not calculate all the data items.
 */
public class StreamHotSpotManager extends BaseHotSpotManager implements DealHotSpotInterface {



    private BaseBloomDetector bloomDetector;
    private BaseFrequentDetector frequentDetector;

    private HashSet<String> currentHotSpotSet = new HashSet<String>();

    private int bloomFilterSum = 0;

    public StreamHotSpotManager() {
        initConfig();

        bloomDetector = new MultiBloomDetectorImp();
        frequentDetector = new SWFPDetectorImp();
    }

    @Override
    public void initConfig() {
        super.initConfig();
    }

    @Override
    public void handleRegister(String key) {
        if (bloomDetector != null && frequentDetector != null) {
            LocalSpots.candidateColdSpots.remove(key);
            if (bloomDetector.registerItem(key)) {
                if (frequentDetector.registerItem(key, bloomFilterSum)) {
                    if (currentHotSpotSet.contains(key)) {
                        return;
                    }
                    currentHotSpotSet.add(key);
                    dealHotData(key);
                }
            }
        }
    }

    @Override
    public void resetCounter() {
        super.resetCounter();

        String bloomFilterOut = bloomDetector.updateFilterThreshold();
        bloomDetector.resetCounter();

        String frequentCounterOut = frequentDetector.updateFrequentCounter();
        frequentDetector.resetCounter();

        Log.log.info(bloomFilterOut + " | " +  frequentCounterOut);
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
        bloomFilterSum = bloomDetector.getItemSum();
        dealHotData();
        LocalSpots.hotSpotNumber.set(currentHotSpotSet.size());
        currentHotSpotSet.clear();
        dealColdData();
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
