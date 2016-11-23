package com.sdp.manager.hotspotmanager;

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

import com.sdp.config.ConfigManager;
import com.sdp.log.Log;
import com.sdp.manager.hotspotmanager.BaseHotSpotManager;
import com.sdp.manager.hotspotmanager.abstracts.BaseHotSpotManager;
import com.sdp.hotspotdetect.bloom.MultiBloomCounterDetectorImp;
import com.sdp.manager.interfaces.DealHotSpotInterface;

public class MultiBloomCounterContrastManager extends BaseHotSpotManager implements DealHotSpotInterface {
	private static int SLICE_TIME;

	private ExecutorService threadPool = Executors.newCachedThreadPool();
	private String hotSpotPath = String.format(System.getProperty("user.dir") + "/logs/server_%d_hotspot.data",
			ConfigManager.id);

	private MultiBloomCounterDetectorImp frequentDetector;

	private HashSet<String> currentHotSpotSet = new HashSet<String>();

	public MultiBloomCounterContrastManager() {
		initConfig();
		frequentDetector = new MultiBloomCounterDetectorImp();
	}

	/**
	 * Read config, get hot spot detection period.
	 *
	 */
	private void initConfig() {
		SLICE_TIME = (Integer) ConfigManager.propertiesMap.get(ConfigManager.SLICE_TIME);
		Log.log.info("[Hot spot detection period]: " + SLICE_TIME);
	}
	@Override
	public void handleRegister(String key) {
		if (frequentDetector != null) {
			System.out.println("处理数据");
			if (currentHotSpotSet.contains(key)) {
				frequentDetector.registerItem(key, 0);
			} else {
				boolean hotOrNot = frequentDetector.registerItem(key, 1);
				if (hotOrNot) {
					currentHotSpotSet.add(key);
					dealHotData(key);
				}
			}
		}
	}

	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			try {
				frequentDetector.resetCounter();
				Thread.sleep(SLICE_TIME);

				write2fileBackground();
                System.out.println(currentHotSpotSet);
				dealHotData();
				currentHotSpotSet.clear();
				//dealColdData();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public void write2fileBackground() {
		System.out.println("开始写文件");
		ArrayList<String> li = new ArrayList<String>(currentHotSpotSet);
		for(int i = 0;i < li.size();i++) {
			String str = li.get(i);
			int num = frequentDetector.findBloomNumber(str);
			frequentDetector.itemCounters.put(str, num);
		}
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
