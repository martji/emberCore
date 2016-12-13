package com.sdp.manager.hotspotmanager;

import com.sdp.hotspotdetect.bloom.CounterBloomDetectorImp;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;

/**
 * CounterBloomHotSpotManager implement {@link BaseHotSpotManager} and the inner hot spot detector is
 * {@link CounterBloomDetectorImp}
 */

public class CounterBloomHotSpotManager extends BaseHotSpotManager implements DealHotSpotInterface {

    private CounterBloomDetectorImp frequentDetector;

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

        frequentDetector.resetCounter();
    }

    @Override
    public void recordHotSpot() {
        super.recordHotSpot();
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
