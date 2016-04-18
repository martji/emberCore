package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.replicas.CallBack;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by magq on 16/1/12.
 */
public class HotspotDetector extends BaseHotspotDetector implements Runnable, CallBack {

    private int log_sleep_time;

    private BaseFrequentDetector frequentDetector;
    private BaseBloomDetector multiBloomDetector;

    private ConcurrentLinkedQueue<String> hotspots = new ConcurrentLinkedQueue<String>();
    private HashSet<String> currentHotspotSet = new HashSet<String>();

    private int preBloomSum = 0;
    private int preSum = 0;

    public HotspotDetector() {
        multiBloomDetector = new MultiBloomDetectorImp();
        frequentDetector = new EcDetectorImp();
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
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while (true) {
            try {
                // 多重布隆过滤器
                ((MultiBloomDetectorImp)multiBloomDetector).updateItemsum(preBloomSum);
                ((MultiBloomDetectorImp)multiBloomDetector).resetCounter();

                // frequent + EC 算法
                ((EcDetectorImp)frequentDetector).updateItemsum(preSum);
                frequentDetector.resetCounter();
//                ((SWFPDetectorImp)frequentDetector).refreshSWFPCounter();

                Thread.sleep(log_sleep_time);


                preBloomSum = ((MultiBloomDetectorImp)multiBloomDetector).itemSum;
                System.out.print(df.format(new Date()) + ": [bloom visit count] " + ((MultiBloomDetectorImp)multiBloomDetector).itemPass +" / "+ preBloomSum);

                preSum = ((EcDetectorImp)frequentDetector).itemSum;
            	System.out.println(" [visit count] " + frequentDetector.itemCounters.size() +" / "+ preSum);


//                List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(frequentDetector.getItemCounters().entrySet());
//                Collections.sort(list, new Comparator<ConcurrentHashMap.Entry<String, Integer>>() {
//                    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
//                        return (o2.getValue() - o1.getValue());
//                    }
//                });
//                System.out.print("[Current frequent items]:");
//                for (Map.Entry<String, Integer> mapping : list) {
//                    System.out.print(mapping.getKey() + "= " + mapping.getValue() + "  ");
//                }
//                System.out.println();
//                System.out.println(currentHotspotSet);
//                for (Iterator it = currentHotspotSet.iterator(); it.hasNext(); ) {
//                    String str = (String) it.next();
//                    System.out.println(str + multiBloomDetector.getHashIndex(str) + "www");
//                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
        isFrequentItem = multiBloomDetector.registerItem(key);

        if (isFrequentItem) {
            if (frequentDetector.registerItem(key)) {
                currentHotspotSet.add(key);
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
