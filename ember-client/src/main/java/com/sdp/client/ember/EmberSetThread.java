package com.sdp.client.ember;


import com.sdp.client.interfaces.DataClient;

import java.util.Vector;
import java.util.concurrent.CountDownLatch;

/**
 * @author Guoqing
 */

public class EmberSetThread implements Runnable {

    private EmberDataClient client;
    private String key;
    private String value;
    private CountDownLatch latch;
    private Vector<Boolean> values;

    public EmberSetThread(DataClient client, String key, String value, CountDownLatch latch, Vector<Boolean> values) {
        this.client = (EmberDataClient) client;
        this.key = key;
        this.value = value;
        this.latch = latch;
        this.values = values;
    }

    @Override
    public void run() {
        boolean result = client.set2DataServer(key, value);
        values.add(result);
        latch.countDown();
    }
}
