package com.sdp.replicas;

import com.sdp.server.DataClient;

import java.util.Vector;
import java.util.concurrent.CountDownLatch;

/**
 * @author magq
 */

public class MCSetThread implements Runnable {

    private DataClient client;
    private String key;
    private String value;
    private CountDownLatch latch;
    private Vector<Boolean> values;

    public MCSetThread(DataClient client, String key, String value, CountDownLatch latch, Vector<Boolean> values) {
        this.client = client;
        this.key = key;
        this.value = value;
        this.latch = latch;
        this.values = values;
    }

    @Override
    public void run() {
        boolean result = client.set(key, value);
        values.add(result);
        latch.countDown();
    }
}
