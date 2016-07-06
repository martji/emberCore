package com.sdp.hotspot;

/**
 * Created by magq on 16/1/18.
 */
public abstract class BaseHotspotDetector implements Runnable {

    public MCallBack callBack;

    public BaseHotspotDetector() {}

    public void handleRegister(String key) {}

    public void finishDealHotSpot(String key) {}

    public void setCallBack(MCallBack callBack) {
        this.callBack = callBack;
    }

    public interface MCallBack {
        void dealHotSpot();
        void dealColdSpot();
        void dealHotSpot(String key);
    }
}
