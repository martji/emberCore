package com.sdp.hotspot;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by magq on 16/1/12.
 */
public class HotspotDetector implements Runnable {

    private static int SLICE_TIME = 1*1000;

    private ConcurrentLinkedQueue<String> hotspots = new ConcurrentLinkedQueue<String>();

    public HotspotDetector() {
        initConfig();
    }

    /**
     * run period
     */
    public void run() {
        try {
            Thread.sleep(SLICE_TIME);

            // update hotspots
            BloomDetectorImp.getInstance().getHotSpots();

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
}
