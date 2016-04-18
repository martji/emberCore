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
        while (true) {
            try {
            	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                preSum = ((EcDetectorImp)frequentDetector).itemSum;
//            	System.out.println(df.format(new Date()) + ": [visit count] " + frequentDetector.itemCounters.size() +" / "+ preSum);
//
//
////                List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(frequentDetector.getItemCounters().entrySet());
////                Collections.sort(list, new Comparator<ConcurrentHashMap.Entry<String, Integer>>() {
////                    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
////                        return (o2.getValue() - o1.getValue());
////                    }
////                });
////                System.out.print("[Current frequent items]:");
////                for (Map.Entry<String, Integer> mapping : list) {
////                    System.out.print(mapping.getKey() + "= " + mapping.getValue() + "  ");
////                }
////                System.out.println();
////                System.out.println(currentHotspotSet);
////                for (Iterator it = currentHotspotSet.iterator(); it.hasNext(); ) {
////                    String str = (String) it.next();
////                    System.out.println(str + multiBloomDetector.getHashIndex(str) + "www");
////                }
//
//                frequentDetector.resetCounter();
//
//                Thread.sleep(log_sleep_time);
//
//                ((EcDetectorImp)frequentDetector).updateItemsum(preSum);
////                ((SWFPDetectorImp)frequentDetector).refreshSWFPCounter();




                preSum = ((MultiBloomDetectorImp)multiBloomDetector).itemSum;
                System.out.println(df.format(new Date()) + ": [visit count] " + ((MultiBloomDetectorImp)multiBloomDetector).itemPass +" / "+ preSum);
                Thread.sleep(log_sleep_time);
                ((MultiBloomDetectorImp)multiBloomDetector).updateItemsum(preSum);

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
//        isFrequentItem = frequentDetector.registerItem(key);

        multiBloomDetector.registerItem(key);

//        if (isFrequentItem) {
//            if (MultiBloomDetectorImp.getInstance().registerItem(key)) {
//                // TODO
//                currentHotspotSet.add(key);
//                MultiBloomDetectorImp.getInstance().resetBloomCounter(key);
//                dealHotData(key);
//            }
//        }
//        if (isFrequentItem) {
//            if (multiBloomDetector.registerItem(key)) {
//                currentHotspotSet.add(key);
//                multiBloomDetector.resetBloomCounter(key);
//                dealHotData(key);
//            }
//        }
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
