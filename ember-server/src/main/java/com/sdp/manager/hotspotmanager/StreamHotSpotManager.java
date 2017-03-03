package com.sdp.manager.hotspotmanager;

import com.sdp.hotspotdetect.bloom.MultiBloomDetectorImp;
import com.sdp.hotspotdetect.frequent.SWFPDetectorImp;
import com.sdp.hotspotdetect.interfaces.BloomDetectorInterface;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;
import com.sdp.manager.ReplicaManager;
import com.sdp.utils.ConstUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by magq on 16/7/6.
 * StreamHotSpotManager implement {@link BaseHotSpotManager} and detects the hot spots by a
 * 3-steps stream mining algorithm. This algorithm contains three steps: data sampling, data filter
 * and data counter.
 * <p>
 * The data sampling is done by the ember client side and the sampling percentage can be configured.
 * <p>
 * The data filter{@link BloomDetectorInterface} is based on the 20/80 theory, and the hot spots must be in
 * the 20%.
 * <p>
 * The data counter{@link FrequentDetectorInterface} intends to find the hot spots by counting the data items,
 * but it does not calculate all the data items.
 */
public class StreamHotSpotManager extends BaseHotSpotManager {

    private MultiBloomDetectorImp bloomDetector;
    private SWFPDetectorImp frequentDetector;

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
        super.handleRegister(key);

        if (bloomDetector != null && frequentDetector != null) {
            if (bloomDetector.registerItem(key)) {
                if (frequentDetector.registerItem(key)) {
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
        bloomDetector.updateFilterThreshold();
        bloomDetector.resetCounter();

        frequentDetector.setRequestNum(requestNum);
        frequentDetector.updateThreshold();
        frequentDetector.resetCounter();

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

    @Override
    public void resetHotData(String key) {
        bloomDetector.resetCounter(key);
        frequentDetector.resetCounter(key);

        super.resetHotData(key);
    }

    @Override
    public void updateHotspotThreshold() {
        super.updateHotspotThreshold();

        frequentDetector.getMoreHotspot(ReplicaManager.unbalanceRatio / ConstUtil.UNBALANCE_THRESHOLD);
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
