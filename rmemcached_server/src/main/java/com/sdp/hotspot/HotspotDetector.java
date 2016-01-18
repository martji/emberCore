package com.sdp.hotspot;

import com.sdp.replicas.CallBack;

import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by magq on 16/1/12.
 */
public class HotspotDetector extends BaseHotspotDetector implements Runnable, CallBack {

    private static int SLICE_TIME = 1*1000;
    private CallBack callBack;

    private ConcurrentLinkedQueue<String> hotspots = new ConcurrentLinkedQueue<String>();
    private HashSet<String> currentHotspotSet = new HashSet<String>();

    public HotspotDetector() {
        initConfig();
    }

    public HotspotDetector(CallBack callBack) {
        this();
        this.callBack = callBack;
    }

    /**
     * run period
     */
    public void run() {
        try {
            Thread.sleep(SLICE_TIME);

            // update hotspots
            callBack.dealHotData();

            // reset counters
            MultiBloomDetectorImp.getInstance().resetBloomCounters();
        } catch (Exception e) {
            // How can it be?
        }
    }

    /**
     * handle register signal
     * @param key
     */
    @Override
    public void handleRegister(String key) {
        if (currentHotspotSet.contains(key)) {
            return;
        }
        boolean isFrequentItem = false;
        isFrequentItem = FrequentDetectorImp.getInstance().registerItem(key);

        if (isFrequentItem) {
            if (MultiBloomDetectorImp.getInstance().registerItem(key)) {
                currentHotspotSet.add(key);
                MultiBloomDetectorImp.getInstance().resetBloomCounter(key);
                // TODO
                dealHotData(key);
            }
        }
    }

    /**
     * read config
     */
    private void initConfig() {

    }

    public void dealHotData() {
        callBack.dealHotData();
    }

    public void dealColdData() {
        callBack.dealColdData();
    }

    public void dealHotData(String key) {
        callBack.dealHotData(key);
    }


    public void removeHotspot(String key) {
        currentHotspotSet.remove(key);
    }
}
