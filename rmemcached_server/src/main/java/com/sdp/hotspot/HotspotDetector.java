package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.replicas.CallBack;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by magq on 16/1/12.
 */
public class HotspotDetector extends BaseHotspotDetector implements Runnable, CallBack {

    private int log_sleep_time;

    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String hotspotPath = String.format(System.getProperty("user.dir") + "/logs/server_%d_hotspot.data", GlobalConfigMgr.id);

    private BaseFrequentDetector frequentDetector;
    private BaseBloomDetector multiBloomDetector;

    private ConcurrentLinkedQueue<String> hotspots = new ConcurrentLinkedQueue<String>();
    private HashSet<String> currentHotspotSet = new HashSet<String>();

    private int preBloomSum = 0;
    private int preSum = 0;

    public HotspotDetector() {
        multiBloomDetector = new MultiBloomDetectorImp();
        frequentDetector = new SWFPDetectorImp();
//        frequentDetector = new EcDetectorImp();
        initConfig();
    }

    /**
     * read config
     *
     */
    private void initConfig() {
        log_sleep_time = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.SLICE_TIME);
        System.out.println("[Log period]: " + log_sleep_time);
    }

    /**
     * run period
     */
    public void run() {
        while (true) {
            try {
                // 多重布隆过滤器
                ((MultiBloomDetectorImp)multiBloomDetector).updateItemsum(preBloomSum);
                ((MultiBloomDetectorImp)multiBloomDetector).resetCounter();

                // frequent + EC 算法
                //((EcDetectorImp)frequentDetector).updateItemsum(preSum);
                //frequentDetector.resetCounter();
                ((SWFPDetectorImp)frequentDetector).updateItemsum(preSum);
                frequentDetector.resetCounter();
                ((SWFPDetectorImp)frequentDetector).refreshSWFPCounter();
                Thread.sleep(log_sleep_time);

                preBloomSum = ((MultiBloomDetectorImp)multiBloomDetector).itemSum;
                
                List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(frequentDetector.getItemCounters().entrySet());
                write2fileBackground(list);

                System.out.println();
                System.out.print(df.format(new Date()) + ": [bloom visit count] " + ((MultiBloomDetectorImp)multiBloomDetector).itemPass +" / "+ preBloomSum);

                preSum = ((SWFPDetectorImp)frequentDetector).itemSum;
            	System.out.println(" [visit count] " + frequentDetector.itemCounters.size() +" / "+ preSum);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void write2fileBackground(final List<Map.Entry<String, Integer>> list) {
        threadPool.execute(new Runnable() {
            public void run() {
                Collections.sort(list, new Comparator<ConcurrentHashMap.Entry<String, Integer>>() {
                    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                        return (o2.getValue() - o1.getValue());
                    }
                });

                try {
                    File file = new File(hotspotPath);
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));

                    bw.write(df.format(new Date()) + " [Current frequent items]:");
                    for (Map.Entry<String, Integer> mapping : list) {
                        bw.write(mapping.getKey() + "= " + mapping.getValue() + "  ");
                    }
                    bw.write("\n\n\n");

                    bw.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
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
        boolean isFrequentItem = true;
        isFrequentItem = multiBloomDetector.registerItem(key);

        if (isFrequentItem) {
            if (frequentDetector.registerItem(key)) {
                // todo
//                currentHotspotSet.add(key);
//                dealHotData(key);
            }
        }
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
