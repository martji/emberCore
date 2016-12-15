package com.sdp.client.reedsolomon;

import com.sdp.client.interfaces.DataClient;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Guoqing on 2016/12/15.
 */
public class RSSetThread implements Callable<Boolean> {

    private DataClient client;
    private String key;
    private String value;
    private CountDownLatch latch;

    public RSSetThread(DataClient client, String key, String value, CountDownLatch latch) {
        this.client = client;
        this.key = key;
        this.value = value;
        this.latch = latch;
    }

    @Override
    public Boolean call() throws Exception {
        boolean result = client.set(key, value);
        latch.countDown();
        return result;
    }
}
