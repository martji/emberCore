package com.sdp.manager.hotspotmanager;

import com.sdp.config.ConfigManager;
import com.sdp.log.Log;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by magq on 16/1/18.
 * Detect the hot spots in requests stream, two managers have been implemented:
 * {@link CounterHotSpotManager} and {@link StreamHotSpotManager}.
 *
 * The core processes are implement in the single thread: resetCounter -> sleep -> write2file -> dealData.
 */
public abstract class BaseHotSpotManager implements Runnable {

    private static int SLICE_TIME;

    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private String hotSpotPath = String.format(System.getProperty("user.dir") +
            "/logs/server_%d_hot_spot.data", ConfigManager.id);

    public OnFindHotSpot onFindHotSpot;

    public BaseHotSpotManager() {}

    /**
     * Run period to update parameters.
     */
    public void run() {
        while (true) {
            try {
                resetCounter();

                Thread.sleep(SLICE_TIME);

                write2fileBackground();

                dealData();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Read config, get hot spot detection period.
     *
     */
    public void initConfig() {
        SLICE_TIME = (Integer) ConfigManager.propertiesMap.get(ConfigManager.SLICE_TIME);
        Log.log.info("[Hot spot detection period]: " + SLICE_TIME);
    }

    /**
     * Handle register signal.
     * @param key
     */
    public void handleRegister(String key) {}

    public void resetCounter() {}

    public void write2fileBackground() {}

    /**
     * Write the current hot spots to file.
     */
    public void write2file(final List<Map.Entry<String, Integer>> list) {
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

    public void dealData() {}

    public void setOnFindHotSpot(OnFindHotSpot onFindHotSpot) {
        this.onFindHotSpot = onFindHotSpot;
    }

    public interface OnFindHotSpot {
        void dealHotSpot();
        void dealHotSpot(String key);
        void dealColdSpot();
    }
}
