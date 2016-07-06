package com.sdp.hotspot;

/**
 * Created by magq on 16/1/18.
 */
public abstract class BaseHotspotDetector implements Runnable {

    public OnFindHotSpot onFindHotSpot;

    public BaseHotspotDetector() {}

    public void handleRegister(String key) {}

    public void finishDealHotSpot(String key) {}

    public void setOnFindHotSpot(OnFindHotSpot onFindHotSpot) {
        this.onFindHotSpot = onFindHotSpot;
    }

    public interface OnFindHotSpot {
        void dealHotSpot();
        void dealColdSpot();
        void dealHotSpot(String key);
    }
}
