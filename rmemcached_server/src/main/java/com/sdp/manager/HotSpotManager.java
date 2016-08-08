package com.sdp.manager;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.example.Log;
import com.sdp.hotspot.BaseHotspotDetector;
import com.sdp.hotspot.MultiBloomDetectorImp;
import com.sdp.hotspot.SWFPDetectorImp;
import com.sdp.replicas.DealHotSpotInterface;
import com.sdp.replicas.LocalSpots;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by magq on 16/7/6.
 */
public class HotSpotManager extends BaseHotspotDetector implements DealHotSpotInterface {

    private static int SLICE_TIME;

    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private String hotSpotPath = String.format(System.getProperty("user.dir") +
            "/logs/server_%d_hot_spot.data", GlobalConfigMgr.id);

    private MultiBloomDetectorImp multiBloomDetector;
    private SWFPDetectorImp frequentDetector;

    private HashSet<String> currentHotSpotSet = new HashSet<String>();

    private int bloomFilterSum = 0;

    public HotSpotManager() {
        initConfig();

        multiBloomDetector = new MultiBloomDetectorImp();
        frequentDetector = new SWFPDetectorImp();
    }

    /**
     * Read config, get hot spot detection period.
     *
     */
    private void initConfig() {
        SLICE_TIME = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.SLICE_TIME);
        Log.log.info("[Hot spot detection period]: " + SLICE_TIME);
    }

    /**
     * Handle register signal.
     * @param key
     */
    @Override
    public void handleRegister(String key) {
        if (multiBloomDetector != null && frequentDetector != null) {
            LocalSpots.candidateColdSpots.remove(key);
            if (multiBloomDetector.registerItem(key)) {
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

    /**
     * Run period to update parameters.
     */
    public void run() {
        while (true) {
            try {
                // multi bloom filter refresh
                String bloomFilterOut = multiBloomDetector.updateItemSum();
                multiBloomDetector.resetCounter();

                // frequent counter refresh
                String frequentCounterOut = frequentDetector.updateItemSum();
                frequentDetector.resetCounter();
                frequentDetector.refreshSWFPCounter();

                Log.log.info(bloomFilterOut + frequentCounterOut);

                Thread.sleep(SLICE_TIME);

                write2fileBackground();

                bloomFilterSum = multiBloomDetector.itemSum;

                dealHotData();

                LocalSpots.hotSpotNumber.set(currentHotSpotSet.size());
                currentHotSpotSet.clear();
                dealColdData();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Record the current hot spots.
     */
    public void write2fileBackground() {
        final List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>
                (frequentDetector.getItemCounters().entrySet());
        if (list != null && list.size() > 0) {
            threadPool.execute(new Runnable() {
                public void run() {
                    Collections.sort(list, new Comparator<ConcurrentHashMap.Entry<String, Integer>>() {
                        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                            return (o2.getValue() - o1.getValue());
                        }
                    });

                    try {
                        File file = new File(hotSpotPath);
                        if (!file.exists()) {
                            file.createNewFile();
                        }
                        BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));

                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        bw.write(df.format(new Date()) + " [Current frequent items]:\n");
                        for (Map.Entry<String, Integer> mapping : list) {
                            bw.write(mapping.getKey() + " = " + mapping.getValue() + "\n");
                        }
                        bw.write("\n\n\n");

                        bw.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
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
