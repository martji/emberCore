package com.sdp.manager.topk;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.log.Log;
import com.sdp.hotspot.abstracts.BaseHotspotDetector;
import com.sdp.hotspot.topk.TopKFrequentDetectorImp;
import com.sdp.manager.interfaces.DealHotSpotInterface;

public class TopKContrastManager extends BaseHotspotDetector implements DealHotSpotInterface {

	private static int SLICE_TIME;

	private ExecutorService threadPool = Executors.newCachedThreadPool();
	private String hotSpotPath = String.format(System.getProperty("user.dir") + "/logs/server_%d_hotspot.data",
			GlobalConfigMgr.id);

	private TopKFrequentDetectorImp frequentDetector;

	private HashSet<String> currentHotSpotSet = new HashSet<String>();

	public TopKContrastManager() {
		initConfig();
		frequentDetector = new TopKFrequentDetectorImp();
	}

	private void initConfig() {
		SLICE_TIME = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.SLICE_TIME);

		Log.log.info("[Hot spot detection period]: " + SLICE_TIME);
	}

	public void handleRegister(String key) {
		if (frequentDetector != null) {
			if ((frequentDetector.registerItem(key, 0)) && (!currentHotSpotSet.contains(key))) {
				currentHotSpotSet.add(key);
				dealHotData(key);
			}
		}
	}

	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			try {
				// frequent counter refresh
				String frequentCounterOut = frequentDetector.updateItemSum();
				frequentDetector.resetCounter();
				Log.log.info(frequentCounterOut + "\n");
				Thread.sleep(SLICE_TIME);

				write2fileBackground();

				dealHotData();
				currentHotSpotSet.clear();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Record the current hot spots.
	 */
	public void write2fileBackground() {
		final List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(
				frequentDetector.getItemCounters().entrySet());
		if (list != null && list.size() > 0) {
			threadPool.execute(new Runnable() {
				public void run() {
					Collections.sort(list, new Comparator<ConcurrentHashMap.Entry<String, Integer>>() {
						public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
							return (o2.getValue() - o1.getValue());
						}
					});

					try {
						File file = new File(hotSpotPath);
						if (!file.exists()) {
							file.createNewFile();
						}
						BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));

						SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						bw.write(df.format(new Date()) + " [Current frequent items]:\n");
						for (Map.Entry<String, Integer> mapping : list) {
							bw.write(mapping.getKey() + " = " + mapping.getValue() + "\n");
						}
						bw.write("\n\n\n");

						bw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}
	}

	public void dealHotData() {
		onFindHotSpot.dealHotSpot();
	}

	public void dealColdData() {
		onFindHotSpot.dealColdSpot();
	}

	public void dealHotData(String key) {
		onFindHotSpot.dealHotSpot(key);
	}

}
