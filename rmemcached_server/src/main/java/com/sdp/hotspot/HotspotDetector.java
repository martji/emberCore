package com.sdp.hotspot;

import com.sdp.replicas.CallBack;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by magq on 16/1/12.
 */
public class HotspotDetector implements Runnable, CallBack {

    private static int SLICE_TIME = 1*1000;
    private CallBack callBack;

    private ConcurrentLinkedQueue<String> hotspots = new ConcurrentLinkedQueue<String>();

    public HotspotDetector() {
        initConfig();
        BloomDetectorImp.getInstance(this);
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
            BloomDetectorImp.getInstance().getHotSpots();
            callBack.dealHotData();

            // reset counters
            BloomDetectorImp.getInstance().resetBloomCounters();
        } catch (Exception e) {
            // How can it be?
        }
    }

    /**
     * handle register signal
     * @param key
     */
    public void handleRegister(String key) {
        boolean isFrequentItem = false;
        isFrequentItem = FrequentDetectorImp.getInstance().registerItem(key);

        if (isFrequentItem) {
            BloomDetectorImp.getInstance().registerItem(key);
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
}
