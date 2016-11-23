package com.sdp.server.dataclient;

import com.sdp.server.DataClient;

/**
 * Created by Guoqing on 2016/11/21.
 * Implement {@link DataClient} with {@link RedisDataClient}.
 */
public class RedisDataClient implements DataClient {

    public String get(String key) {
        return null;
    }

    public boolean set(String key, String value) {
        return false;
    }

    public static RedisDataClient createInstance(int id) {
        return null;
    }
}
