package com.sdp.hotspot;

import com.sdp.config.GlobalConfigMgr;

/**
 * Created by magq on 16/1/13.
 */
public class BloomCounter implements Runnable{

    private int primaryCounter = 0;
    private int secondaryCounter = 0;

    private static int COUNTER_PERIOD = 10*1000;

    public BloomCounter() {
        initConfig();
        run();
    }

    private void initConfig () {
        COUNTER_PERIOD = (Integer) GlobalConfigMgr.configMap.get(GlobalConfigMgr.COUNTER_PERIOD);
    }

    public void resetCounter() {
        int tmp = primaryCounter;
        secondaryCounter += primaryCounter;
        primaryCounter = tmp / 2;
    }

    public int visit() {
        return ++primaryCounter + secondaryCounter / 2;
    }

    public int getVisitCounter() {
        return primaryCounter + secondaryCounter / 2;
    }

    public void resetVisitCounter(int minCounter) {
        if (primaryCounter >= minCounter) {
            primaryCounter -= minCounter;
        } else {
            secondaryCounter -= (minCounter - primaryCounter);
            primaryCounter = 0;
        }
    }

    public void run() {
        try {
            resetCounter();
            Thread.sleep(COUNTER_PERIOD);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
