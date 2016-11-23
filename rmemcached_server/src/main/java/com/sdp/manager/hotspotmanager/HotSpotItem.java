package com.sdp.manager.hotspotmanager;

/**
 * Created by Guoqing on 2016/11/23.
 */
public class HotSpotItem implements Comparable<HotSpotItem> {

    private String key;
    private int count;

    public HotSpotItem(String key, int count) {
        this.key = key;
        this.count = count;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int compareTo(HotSpotItem that) {
        return that.count - this.count;
    }
}
