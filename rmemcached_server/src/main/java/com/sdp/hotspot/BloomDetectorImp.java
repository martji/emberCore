package com.sdp.hotspot;

import com.sdp.replicas.CallBack;

import java.util.Vector;

/**
 * Created by magq on 16/1/12.
 */
public class BloomDetectorImp implements BaseBloomDetector {

    private static BloomDetectorImp ourInstance = null;
    private CallBack callBack;

    private Vector<BloomCounter> bloomCounterMap = new Vector<BloomCounter>();

    private static int HOTSPOT_THRESHOLD = 100;

    public static BloomDetectorImp getInstance() {
        if (ourInstance == null) {
            ourInstance = new BloomDetectorImp();
            ourInstance.initConfig();
        }
        return ourInstance;
    }

    public static BloomDetectorImp getInstance(CallBack callBack) {
        if (ourInstance == null) {
            ourInstance = new BloomDetectorImp();
            ourInstance.initConfig();
            ourInstance.callBack = callBack;
        }
        return ourInstance;
    }

    public void initConfig() {

    }

    public int[] getHashIndex(String key) {
        return new int[0];
    }

    public void registerItem(String key) {
        int[] indexs = getHashIndex(key);
        for (int index : indexs) {
            if (bloomCounterMap.get(index).visit() > HOTSPOT_THRESHOLD) {
                //TODO
            }
        }
    }

    public void resetBloomCounters() {

    }

    public Vector<String> getHotSpots() {
        return null;
    }
}
