package com.sdp.manager.hotspotmanager;

import com.sdp.config.ConfigManager;
import com.sdp.log.Log;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.utils.DataUtil;
import com.sdp.utils.SpotUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by magq on 16/1/18.
 * Detect the hot spots in requests stream, two managers have been implemented:
 * {@link CounterHotSpotManager} and {@link StreamHotSpotManager} and {@link TopKHotSpotManager} and
 * {@link MultiBloomHotSpotManager} and {@link CounterBloomHotSpotManager} and {@link FrequentHotSpotManager}.
 * <p>
 * The core processes are implement in the single thread: resetCounter -> sleep -> write2file -> dealData.
 */
public abstract class BaseHotSpotManager implements Runnable, DealHotSpotInterface {

    private int SLICE_TIME;

    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private String hotSpotPath = String.format(System.getProperty("user.dir") +
            "/logs/server_%d_hot_spot.txt", ConfigManager.id);

    public int requestNum;
    public OnFindHotSpot onFindHotSpot;
    public Set<String> currentHotSpotSet = new ConcurrentSkipListSet<>();

    private int preHotSize = 0;

    public BaseHotSpotManager() {
    }

    /**
     * Run period to update parameters.
     */
    public void run() {
        while (true) {
            try {
                resetCounter();
                Thread.sleep(SLICE_TIME);
                dealData();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Read config, get hot spot detection period.
     */
    public void initConfig() {
        SLICE_TIME = (Integer) ConfigManager.propertiesMap.get(ConfigManager.SLICE_TIME);
        Log.log.info("[HotSpotManager] hot spot detection sliceTime = " + SLICE_TIME);
    }

    /**
     * Handle register signal.
     *
     * @param key
     */
    public void handleRegister(String key) {
        requestNum++;
        SpotUtil.candidateColdSpots.remove(key);
    }

    public void resetCounter() {
        requestNum = 0;
        currentHotSpotSet.clear();
    }

    public void dealData() {
        recordHotSpot();
        dealColdData();

        updateSliceTime();
        updateHotspotThreshold();
    }

    /**
     * update SLICE_TIME
     */
    private void updateSliceTime() {
        SLICE_TIME *= SpotUtil.retireRatio;
    }

    /**
     * update hotspot threshold
     */
    public void updateHotspotThreshold() {
    }

    /**
     * Write the current hot spots to file.
     */
    public void recordHotSpot() {
        Log.log.info("[HotSpotManager] requestNum = " + requestNum +
                ", retireRatio = " + DataUtil.doubleFormat(SpotUtil.retireRatio));
    }

    public void recordCurrentHotSpot(final List<HotSpotItem> list) {
        if (list != null && list.size() > preHotSize) {
            Log.log.info("[HotSpotManager] current hot spot number = " + list.size());
            preHotSize = list.size();
            threadPool.execute(new RecordHotSpotThread(list));
        }
    }

    public void resetHotData(String key) {
        currentHotSpotSet.remove(key);
    }

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
                BufferedWriter bw = new BufferedWriter(new FileWriter(file, false));
                for (HotSpotItem item : hotSpotItemList) {
                    bw.write(item.getKey() + "\t" + item.getCount() + "\n");
                }
                bw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
