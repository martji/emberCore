package com.sdp.hotspotdetect.frequent.topk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import com.sdp.config.ConfigManager;
import com.sdp.hotspotdetect.interfaces.FrequentDetectorInterface;

public class TopKDetectorImp extends Thread implements FrequentDetectorInterface {
	
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

	/**
	 * 1.	采用m个counter对m个item进行监管，相应的counter有一个ε值，对应于代码中的preValue。
	   2.	当计数器更换监管item时，用ε记录下新item到来前的counter的值。计数器当前监管的item的数据项个数肯定大于Counter-ε，充分性
	   3.	处理过程：
	        如果新到来的item已经被监管，则相应计数器加一
	        如果新到来的item没有被监管，暂且相信它为频繁访问的item，找出当前计数器中值最小的item进行替换。该counter的值赋予ε，counter++
	 4.	判定过程：寻找top-k，实际找到的item个数为k’
     5.topElementsList中存储了热点数据，topElementsList数组两秒更新一次
	 */
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

    /**
     * 这个在word里有图。
     */
    public void run(){
        do{
            int minGuarFreq = Integer.MAX_VALUE;
            int i = 0;
            String str = null;
            Iterator iter = currentHotSpotCounters.keySet().iterator();
            topElementsList.clear();
            for(i = 0; i < topItemsNumber && i < currentHotSpotCounters.size(); i++){
                if(iter.hasNext()){
                    str = (String) iter.next();
                    topElementsList.add(str);
                    if(currentHotSpotCounters.get(str) - preValue.get(str) < minGuarFreq){
                        minGuarFreq = currentHotSpotCounters.get(str) - preValue.get(str);
                    }
                }
            }//遍历完之后，第k+1个需要遍历，i=k;
            String strKAdd = (String) iter.next();
            if(iter.hasNext() && currentHotSpotCounters.get(strKAdd) >=  minGuarFreq){
                str = strKAdd;
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

    /**
     * 下面两个方法应该没有用到
     * @return
     */
	/*public ConcurrentHashMap<String, Integer> getCurrentHotSpot() {
		return currentHotSpotCounters;
	}

	public void resetCounter() {
		// TODO Auto-generated method stub
		currentHotSpotCounters.clear();
	}*/

	public String updateFrequentCounter() {
		return null;
	}

	public void refreshSWFPCounter() {

	}

}
