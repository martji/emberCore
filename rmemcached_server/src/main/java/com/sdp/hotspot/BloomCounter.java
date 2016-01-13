package com.sdp.hotspot;

/**
 * Created by magq on 16/1/13.
 */
public class BloomCounter{

    private int primaryCounter = 0;
    private int secondaryCounter = 0;

    public BloomCounter() {
    }

    public void resetCounter() {
        secondaryCounter = primaryCounter;
        primaryCounter = 0;
    }

    public int visit() {
        return ++primaryCounter;
    }

    public int getVisitCounter() {
        return primaryCounter;
    }

    public void resetVisitCounter(int minCounter) {
        primaryCounter -= minCounter;
    }
}
