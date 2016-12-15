package com.sdp.client.reedsolomon;

import com.sdp.client.interfaces.DataClient;

import java.util.Vector;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Guoqing on 2016/12/15.
 */
public class RSGetThread implements Runnable {

    private DataClient client;
    private String key;
    private CountDownLatch latch;
    private Vector<String> values;

    public RSGetThread(DataClient client, String key, CountDownLatch latch, Vector<String> values) {
        this.client = client;
        this.key = key;
        this.latch = latch;
        this.values = values;
    }

    @Override
    public void run() {
        String value = client.get(key);
        values.add(value);
        latch.countDown();
    }
}
