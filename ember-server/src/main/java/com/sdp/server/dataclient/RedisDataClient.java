package com.sdp.server.dataclient;

import com.sdp.server.DataClient;
import redis.clients.jedis.Jedis;

/**
 * Created by Guoqing on 2016/11/21.
 * Implement {@link DataClient} with {@link RedisDataClient}.
 */
public class RedisDataClient implements DataClient {

    private Jedis client;

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
