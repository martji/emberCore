package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.replicas.CallBack;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    	// initConfig();
       // frequentDetector = EcDetectorImp.getInstance();
        //multiBloomDetector = MultiBloomDetectorImp.getInstance();
        frequentDetector = new EcDetectorImp();
        multiBloomDetector = new MultiBloomDetectorImp();
        initConfig();
    }

    /**
     * run period
     */
    public void run() {
        int log_sleep_time = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.SLICE_TIME);
        System.out.println("[Log period]: " + log_sleep_time);
        while (true) {
            try {
            	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
            	System.out.print(df.format(new Date())+"  ");// new Date()为获取当前系统时间
            	System.out.println(frequentDetector.itemCounters.size()+"www"+((EcDetectorImp)frequentDetector).itemSum);
            	List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(frequentDetector.getItemCounters().entrySet());  
            	Collections.sort(list, new Comparator<ConcurrentHashMap.Entry<String, Integer>>(){   
            	    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {      
            	        return (o2.getValue() - o1.getValue()); 
            	    }
            	}); 
            	System.out.print("[Current frequent items]:");
            	 for (Map.Entry<String, Integer> mapping : list) {  
                     System.out.print(mapping.getKey() + "= " + mapping.getValue()+"  ");  
                 }  
            	 System.out.println();
            	 System.out.println(currentHotspotSet);
            	 for(Iterator it=currentHotspotSet.iterator();it.hasNext();)
            	  {
            		 String str = (String) it.next();
            	   System.out.println(str+multiBloomDetector.getHashIndex(str)+"www");
            	  }
                frequentDetector.resetCounter();
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

      /*  if (isFrequentItem) {
            if (MultiBloomDetectorImp.getInstance().registerItem(key)) {
                // TODO
                currentHotspotSet.add(key);
                MultiBloomDetectorImp.getInstance().resetBloomCounter(key);
                dealHotData(key);
            }
        }*/
        /*if(isFrequentItem){
        	if(multiBloomDetector.registerItem(key)){
        		currentHotspotSet.add(key);
        		multiBloomDetector.resetBloomCounter(key);
        		dealHotData(key);
        	}
        }*/
        
    }

    /**
     * read config
     * 
     */
    private void initConfig() {
        /*FrequentDetectorImp.getInstance().initConfig();~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    	 EcDetectorImp.getInstance().initConfig();
        MultiBloomDetectorImp.getInstance().initConfig();*/
    	frequentDetector.initConfig();
    	multiBloomDetector.initConfig();
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
