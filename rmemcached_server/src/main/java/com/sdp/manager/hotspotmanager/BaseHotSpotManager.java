package com.sdp.manager.hotspotmanager;

import com.sdp.config.ConfigManager;
import com.sdp.log.Log;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by magq on 16/1/18.
 * Detect the hot spots in requests stream, two managers have been implemented:
 * {@link CounterHotSpotManager} and {@link StreamHotSpotManager} and {@link TopKHotSpotManager} and
 * {@link MultiBloomHotSpotManager} and {@link CounterBloomHotSpotManager} and {@link FrequentHotSpotManager}.
 *
 * The core processes are implement in the single thread: resetCounter -> sleep -> write2file -> dealData.
 */
public abstract class BaseHotSpotManager implements Runnable {

    private int SLICE_TIME;

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
                recordHotSpot();
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
        Log.log.info("[HotSpot Manager] hot spot detection sliceTime = " + SLICE_TIME);
    }

    /**
     * Handle register signal.
     * @param key
     */
    public void handleRegister(String key) {}

    public void resetCounter() {}

    /**
     * Write the current hot spots to file.
     */
    public void recordHotSpot() {}

    public void recordCurrentHotSpot(final List<HotSpotItem> list) {
        if (list != null && list.size() > 0) {
            threadPool.execute(new RecordHotSpotThread(list));
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

    /**
     * Record the current hot spots in a single thread.
     */
    public class RecordHotSpotThread implements Runnable {

        public List<HotSpotItem> hotSpotItemList;

        public RecordHotSpotThread(List<HotSpotItem> list) {
            this.hotSpotItemList = list;
        }

        public void run() {
            if (hotSpotItemList == null || hotSpotItemList.size() == 0) {
                return;
            }
            Collections.sort(hotSpotItemList);
            try {
                File file = new File(hotSpotPath);
                if (!file.exists()) {
                    file.createNewFile();
                }
                BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                bw.write(df.format(new Date()) + " Current frequent items:\n");
                for (HotSpotItem item : hotSpotItemList) {
                    bw.write(item.getKey() + "\t" + item.getCount() + "\n");
                }
                bw.write("\n");
                bw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
