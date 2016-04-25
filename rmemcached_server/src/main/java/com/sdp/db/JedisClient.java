package com.sdp.db;

import redis.clients.jedis.Jedis;

/**
 * Created by Guoqing on 2016/4/25.
 */
public class JedisClient implements DBClientInterface {

    private Jedis jedis = null;

    public JedisClient() {

    }

    public JedisClient(String host, int port) {
        jedis = new Jedis(host, port);
    }

    public void setDB(String host, int port) {
        jedis = new Jedis(host, port);
    }

    public boolean set(String key, String value) {
        if (jedis != null) {
            return jedis.set(key, value) != null ? true : false;
        }
        return false;
    }

    public String get(String key) {
        if (jedis == null) {
            return null;
        }
        return jedis.get(key);
    }
}
