package com.sdp.hotspot.topk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.ConfigManager;
import com.sdp.hotspot.interfaces.BaseFrequentDetector;

public class TopKDetectorImp extends Thread implements BaseFrequentDetector {
	
	private static int topItemsNumber = 10;
	
	private static int counterNumber = 20;
	
	private static TopKDetectorImp ourInstance = null;

	private ConcurrentHashMap<String, Integer> preValue = new ConcurrentHashMap<String, Integer>();
	 
	private ArrayList topElementsList = new ArrayList();

	/*public static TopKDetectorImp getInstance() {
		if (ourInstance == null) {
			ourInstance = new TopKDetectorImp();
			ourInstance.start();
		}
		return ourInstance;
	}*/

	private TopKDetectorImp() {
		initConfig();
	}

	public void initConfig() {
		topItemsNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.TOP_ITEM_NUMBER);
		counterNumber = (Integer) ConfigManager.propertiesMap.get(ConfigManager.COUNTER_NUMBER);
	}

	public boolean registerItem(String key, int preSum) {
		if(itemCounters.contains(key)){
			itemCounters.put(key, itemCounters.get(key) + 1);
		} else if(itemCounters.size() < counterNumber){
			itemCounters.put(key, 1);
			preValue.put(key, 0);
		}else{
			int min = Integer.MAX_VALUE;
			String strMin = null;
			String str = null;
			Iterator iter = itemCounters.keySet().iterator();
			while(iter.hasNext()){
				str = (String) iter.next();
				if(itemCounters.get(str) < min){
					strMin = str;
					min = itemCounters.get(str);
				}
			}
			itemCounters.remove(strMin);
			preValue.remove(strMin);
			itemCounters.put(key, min + 1);
			preValue.put(key, min);
		}
		if(topElementsList.contains(key)){
			return true;
		}
		else
			return false;
	}

	public void run(){
		do{
			int minGuarFreq = Integer.MAX_VALUE;
			int i = 0;
			String str = null;
			Iterator iter = itemCounters.keySet().iterator();
			for(i = 0;i < topItemsNumber && i < itemCounters.size();i++){
				if(iter.hasNext()){
					str = (String) iter.next();
					topElementsList.add(str);
					if(itemCounters.get(str) - preValue.get(str) < minGuarFreq){
						minGuarFreq = itemCounters.get(str) - preValue.get(str);
					}
				}
			}//遍历完之后，第k+1个需要遍历，i=k;
			if(iter.hasNext() && itemCounters.get((String) iter.next()) >=  minGuarFreq){
				str = iter.toString();
				topElementsList.add(str);
				for(i = i + 1;i < itemCounters.size(); i ++){
					if(itemCounters.get(str) - preValue.get(str) < minGuarFreq){
						minGuarFreq = itemCounters.get(str) - preValue.get(str);
					} 
					str = (String) iter.next();
					if(itemCounters.get(str) <= minGuarFreq){
						break;
					}
					topElementsList.add(str);
				}
			}
			try {
				sleep(2*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}while(true);
	}

	public ConcurrentHashMap<String, Integer> getItemCounters() {
		return itemCounters;
	}
	
	public void resetCounter() {
		// TODO Auto-generated method stub
		itemCounters.clear();
	}

	public String updateItemSum() {
		return null;
	}

	public void refreshSWFPCounter() {

	}

}