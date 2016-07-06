package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.replicas.CallBack;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by magq on 16/1/12.
 */
public class HotspotDetector extends BaseHotspotDetector implements CallBack {

    private static int SLICE_TIME;

    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String hotSpotPath = String.format(System.getProperty("user.dir") + "/logs/server_%d_hotspot.data", GlobalConfigMgr.id);

    private MultiBloomDetectorImp multiBloomDetector;
    private SWFPDetectorImp frequentDetector;

    private HashSet<String> currentHotspotSet = new HashSet<String>();

    private int bloomFilterSum = 0;

    public HotspotDetector() {
        multiBloomDetector = new MultiBloomDetectorImp();
        frequentDetector = new SWFPDetectorImp();
        initConfig();
    }

    /**
     * Read config, get hot spot detection period.
     *
     */
    private void initConfig() {
        SLICE_TIME = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.SLICE_TIME);
        System.out.println("[Hot spot detection period]: " + SLICE_TIME);
    }

    /**
     * Handle register signal.
     * @param key
     */
    @Override
    public void handleRegister(String key) {
        if (currentHotspotSet.contains(key)) {
            return;
        }

        if (multiBloomDetector.registerItem(key)) {
            if (frequentDetector.registerItem(key, bloomFilterSum)) {
                currentHotspotSet.add(key);
                dealHotData(key);
            }
        }
    }

    @Override
    public void finishDealHotSpot(String key) {
        currentHotspotSet.remove(key);
    }

    /**
     * Run period to update parameters.
     */
    public void run() {
        while (true) {
            try {
                // multi bloom filter refresh
                multiBloomDetector.updateItemSum();
                multiBloomDetector.resetCounter();

                // frequent counter refresh
                frequentDetector.updateItemSum();
                frequentDetector.resetCounter();
                frequentDetector.refreshSWFPCounter();

                currentHotspotSet.clear();

                Thread.sleep(SLICE_TIME);

                write2fileBackground();

                bloomFilterSum = multiBloomDetector.itemSum;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

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
        callBack.dealHotSpot();
    }

    public void dealColdData() {
        callBack.dealColdSpot();
    }

    public void dealHotData(String key) {
        callBack.dealHotSpot(key);
    }
}
