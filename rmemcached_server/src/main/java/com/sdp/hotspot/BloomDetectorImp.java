package com.sdp.hotspot;

import java.util.Vector;

/**
 * Created by magq on 16/1/12.
 */
public class BloomDetectorImp implements BaseBloomDetector {

    private static BloomDetectorImp ourInstance = null;

    public static BloomDetectorImp getInstance() {
        if (ourInstance == null) {
            ourInstance = new BloomDetectorImp();
            ourInstance.initConfig();
        }
        return ourInstance;
    }
    public void initConfig() {

    }

    public int[] getHashIndex(String key) {
        return new int[0];
    }

    public void resetBloomCounters() {

    }

    public Vector<String> getHotSpots() {
        return null;
    }
}
