package com.sdp.hotspot;

import com.sdp.monitor.LocalMonitor;
import com.sdp.replicas.LocalSpots;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.*;
import java.util.Map.Entry;

public class HotspotIdentifier extends BaseHotspotDetector implements Runnable{
	Long currenTimestamp;
	int sliceId;
	Queue<String> tmpLowQueue = new LinkedList<String>();
	Map<String, Integer> tmpHighMap = new HashMap<String, Integer>();
	Map<String, RankItem> hotspotMap = new HashMap<String, RankItem>();
	
	final int tmpLowQueueSize = 1000;
	final double cpuThreshold = 80;
	
	ConcurrentHashMap<String, Integer> countMap = new ConcurrentHashMap<String, Integer>();
	ConcurrentHashMap<String, Vector<Integer>> replicasIdMap;
	final int sliceTime = 15*1000;
	
	public HotspotIdentifier (Long timestamp) {
		this.currenTimestamp = timestamp;
		sliceId = 0;
		getNextSlice();
	}
	
	public HotspotIdentifier(ConcurrentHashMap<String, Vector<Integer>> replicasIdMap) {
		super();
		this.replicasIdMap = replicasIdMap;
	}

	public void run() {
		while (true) {
			try {
				Thread.sleep(sliceTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
//			if (LocalSpots.hotspots.keySet() != null) {
//				callBack.dealHotspot();
//			}
//			dealColdData();

			System.out.println("[Count map]:" + countMap);

			countMap = new ConcurrentHashMap<String, Integer>();
		}
	}

	private void dealColdData() {
		if (replicasIdMap != null && replicasIdMap.size() > 0) {
			Set<String> hotItems = replicasIdMap.keySet();
			for (String key : hotItems) {
				if (!LocalSpots.containsHot(key)) {
					if (!countMap.containsKey(key) || 
							countMap.get(key) < LocalSpots.coldThreshold) { // LocalSpots.threshold / 2 / replicasIdMap.get(key).size()
						LocalSpots.addCold(key);
					}
				}
			}
			if (LocalSpots.coldspots.keySet() != null) {
				callBack.dealColdspot();
			}
		}
	}

	@Override
	public void handleRegister(String key) {
		if (!countMap.containsKey(key)) {
			countMap.put(key, 0);
		}
		int visits = countMap.get(key) + 1;
		if (visits >= LocalSpots.threshold && !LocalSpots.containsHot(key)) {
			LocalSpots.addHot(key);
			countMap.put(key, visits);
		} else {
			countMap.put(key, visits);
		}
	}
	
	public void resetVisit(String key) {
		if (countMap.containsKey(key)) {
			int count = countMap.get(key);
			int size = replicasIdMap.get(key).size();
			count = count * (size - 1) /  size;
			countMap.put(key, count);
		}
	}
	
	public void handleRegister(Long timestamp, String key) {
		if (timestamp < currenTimestamp) {
			if (tmpHighMap.containsKey(key)) {
				tmpHighMap.put(key, tmpHighMap.get(key) + 1);
			} else if (tmpLowQueue.contains(key)) {
				tmpHighMap.put(key, 2);
			} else {
				tmpLowQueue.offer(key);
				if (tmpHighMap.size() > tmpLowQueueSize) {
					tmpLowQueue.poll();
				}
			}
		} else {
			Map<String, Integer> highMap = new HashMap<String, Integer>();
			for (Entry<String, Integer> e : tmpHighMap.entrySet()) {
				highMap.put(e.getKey(), e.getValue());
			}
			caculaterHotspot(sliceId, highMap);
			
			tmpLowQueue = new LinkedList<String>();
			tmpHighMap = new HashMap<String, Integer>();
			getNextSlice();
		}
	}
	
	private void caculaterHotspot(final int sliceId, final Map<String, Integer> highMap) {
		Runnable runnable = new Runnable() {
			public void run() {
				for (Entry<String, Integer> e : highMap.entrySet()) {
					String key = e.getKey();
					int count = e.getValue();
					CountItem countItem = new CountItem(sliceId, count);
					if (hotspotMap.containsKey(key)) {
						hotspotMap.get(key).addCount(countItem);
					} else {
						RankItem rankItem = new RankItem(key);
						rankItem.addCount(countItem);
						hotspotMap.put(key, rankItem);
					}
				}
				
				if (LocalMonitor.getInstance().cpuCost > cpuThreshold) {
					String hotKey = null;
					Double weight = 0.0;
					for (Entry<String, Integer> e : highMap.entrySet()) {
						String key = e.getKey();
						RankItem rankItem = hotspotMap.get(key);
						if (rankItem.getWeight(sliceId) > weight) {
							weight = rankItem.getWeight(sliceId);
							hotKey = key;
						}
					}
					if (hotKey != null) {
						// ?
					}
				}
			}
		};
		new Thread(runnable).start();
	}

	public void getNextSlice() {
		currenTimestamp += 5*1000;
		sliceId += 1;
	}
}
