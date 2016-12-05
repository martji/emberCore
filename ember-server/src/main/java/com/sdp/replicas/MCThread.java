package com.sdp.replicas;

import com.sdp.server.DataClient;

import java.util.concurrent.Callable;

/**
 * @author magq
 */

public class MCThread implements Callable<Boolean> {

    DataClient mClient;
    String key;
    String value;

    public MCThread(DataClient mClient, String key, String value) {
        this.mClient = mClient;
        this.key = key;
        this.value = value;
    }

    public Boolean call() throws Exception {
        return mClient.set(key, value);
    }
}
