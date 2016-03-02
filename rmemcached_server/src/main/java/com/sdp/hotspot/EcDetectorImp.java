package com.sdp.hotspot;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.GlobalConfigMgr;

public class EcDetectorImp extends Thread implements BaseFrequentDetector {

    private static double frequentPercentage =  0.1;
	
	private static int counterNumber = 20;
	
	private static double errorRate = 0.01;
	
	private static int itemSum = 0;
	
	private static EcDetectorImp ourInstance = null;

    private ConcurrentHashMap<String, Integer> itemPreCounters = new ConcurrentHashMap<String, Integer>();
	
    private ConcurrentHashMap<String, Integer> itemSumCounters = new ConcurrentHashMap<String, Integer>();

    
    public static EcDetectorImp getInstance() {
        if (ourInstance == null) {
            ourInstance = new EcDetectorImp();
            ourInstance.start();
        }
        return ourInstance;
    }

    private EcDetectorImp() {
        initConfig();
    }
    
    public void initConfig() {
		// TODO Auto-generated method stub
		frequentPercentage =  (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.FREQUENT_PERCENTAGE);
    	errorRate = (Double) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.ERROR_RATE);
    	counterNumber = (int) (1/errorRate);
	}

	public boolean registerItem(String key) {
		// TODO Auto-generated method stub
		itemSum ++;
		if(itemCounters.contains(key)){
			itemCounters.put(key, itemCounters.get(key) + 1);
		}else if(itemCounters.size() < counterNumber){
			itemCounters.put(key, 1);
			itemPreCounters.put(key, 0);
			itemSumCounters.put(key, itemSum);
		}else{
			while(!itemCounters.containsValue(0)){
				Iterator iter = itemCounters.keySet().iterator();
				String str = null;
	            while(iter.hasNext()){
	            	str = (String) iter.next();
	            	itemCounters.put(str, itemCounters.get(str) - 1);
	            	itemPreCounters.put(str, itemPreCounters.get(str) + 1);
	            }
			}
			Iterator iter = itemCounters.keySet().iterator();
			String str = null;
			while(iter.hasNext()){
				str = (String) iter.next();
				if(itemCounters.get(str) == 0){
					itemCounters.remove(str);
					itemPreCounters.remove(str);
					itemSumCounters.remove(str);
				}
			}
			itemCounters.put(key, 1);
			itemPreCounters.put(key, 0);
			itemSumCounters.put(key, itemSum);
		}
		if(itemCounters.get(key) + itemPreCounters.get(key) > (frequentPercentage - errorRate) *itemSum){
			return true;
		}else{
			return false;
		}
	}

	public void run(){
		do{
			itemSum = itemSum/2;
			//int sum = itemSum;
			try {
				sleep(2*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//itemSum = itemSum - sum;
			//这里更新的策略，可以定时减半，或者记录一下前一时刻的总量，然后过段时间后再求差值这样
		}while(true);
	}

	public ConcurrentHashMap<String, Integer> getItemCounters() {
		return itemCounters;
	}
}
