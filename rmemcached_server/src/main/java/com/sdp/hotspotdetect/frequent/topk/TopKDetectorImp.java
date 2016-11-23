package com.sdp.hotspotdetect.frequent.topk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.interfaces.BaseFrequentDetector;

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
		if(currentHotSpotCounters.contains(key)){
			currentHotSpotCounters.put(key, currentHotSpotCounters.get(key) + 1);
		} else if(currentHotSpotCounters.size() < counterNumber){
			currentHotSpotCounters.put(key, 1);
			preValue.put(key, 0);
		}else{
			int min = Integer.MAX_VALUE;
			String strMin = null;
			String str = null;
			Iterator iter = currentHotSpotCounters.keySet().iterator();
			while(iter.hasNext()){
				str = (String) iter.next();
				if(currentHotSpotCounters.get(str) < min){
					strMin = str;
					min = currentHotSpotCounters.get(str);
				}
			}
			currentHotSpotCounters.remove(strMin);
			preValue.remove(strMin);
			currentHotSpotCounters.put(key, min + 1);
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
			Iterator iter = currentHotSpotCounters.keySet().iterator();
			for(i = 0; i < topItemsNumber && i < currentHotSpotCounters.size(); i++){
				if(iter.hasNext()){
					str = (String) iter.next();
					topElementsList.add(str);
					if(currentHotSpotCounters.get(str) - preValue.get(str) < minGuarFreq){
						minGuarFreq = currentHotSpotCounters.get(str) - preValue.get(str);
					}
				}
			}//遍历完之后，第k+1个需要遍历，i=k;
			if(iter.hasNext() && currentHotSpotCounters.get((String) iter.next()) >=  minGuarFreq){
				str = iter.toString();
				topElementsList.add(str);
				for(i = i + 1; i < currentHotSpotCounters.size(); i ++){
					if(currentHotSpotCounters.get(str) - preValue.get(str) < minGuarFreq){
						minGuarFreq = currentHotSpotCounters.get(str) - preValue.get(str);
					} 
					str = (String) iter.next();
					if(currentHotSpotCounters.get(str) <= minGuarFreq){
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

	public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
		return currentHotSpotCounters;
	}
	
	public void resetCounter() {
		// TODO Auto-generated method stub
		currentHotSpotCounters.clear();
	}

	public String updateFrequentCounter() {
		return null;
	}

	public void refreshSWFPCounter() {

	}

}
