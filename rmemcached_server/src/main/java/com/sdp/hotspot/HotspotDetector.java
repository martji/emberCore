package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.replicas.CallBack;

import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by magq on 16/1/12.
 */
public class HotspotDetector extends BaseHotspotDetector implements Runnable, CallBack {

    private static int SLICE_TIME = 1*1000;
    private BaseFrequentDetector frequentDetector;
    private BaseBloomDetector multiBloomDetector;

    private ConcurrentLinkedQueue<String> hotspots = new ConcurrentLinkedQueue<String>();
    private HashSet<String> currentHotspotSet = new HashSet<String>();

    public HotspotDetector() {
        initConfig();
        frequentDetector = EcDetectorImp.getInstance();
        multiBloomDetector = MultiBloomDetectorImp.getInstance();
    }

    /**
     * run period
     */
    public void run() {
        int log_sleep_time = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.SLICE_TIME);
        System.out.println("[Log period]: " + log_sleep_time);
        while (true) {
            try {
                System.out.println("[Current frequent items]: " + frequentDetector.getItemCounters());
                Thread.sleep(log_sleep_time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //TODO
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
        isFrequentItem = frequentDetector.registerItem(key);

        /*
        if (isFrequentItem) {
            if (MultiBloomDetectorImp.getInstance().registerItem(key)) {
                // TODO
                currentHotspotSet.add(key);
                MultiBloomDetectorImp.getInstance().resetBloomCounter(key);
                dealHotData(key);
            }
        }
        */
    }

    /**
     * read config
     */
    private void initConfig() {
        FrequentDetectorImp.getInstance().initConfig();
        MultiBloomDetectorImp.getInstance().initConfig();
    }

    public void dealHotData() {
        callBack.dealHotspot();
    }

    public void dealColdData() {
        callBack.dealColdspot();
    }

    public void dealHotData(String key) {
        callBack.dealHotspot(key);
    }


    public void removeHotspot(String key) {
        currentHotspotSet.remove(key);
    }
}
