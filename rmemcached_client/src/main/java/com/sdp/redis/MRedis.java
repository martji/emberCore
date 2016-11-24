package com.sdp.redis;

import redis.clients.jedis.Jedis;

/**
 * Created by Guoqing on 2016/4/19.
 */
public class MRedis {

    private Jedis jedis;

    public MRedis(String host, int port) {
        jedis = new Jedis(host, port);
    }

    public boolean set(String key, String value) {
        String out = jedis.set(key, value);
        return out != null;
    }

    public String get(String key) {
        return jedis.get(key);
    }

    public void shutdown() {
        jedis.close();
    }

    public static void main(String[] args) {
        MRedis mRedis = new MRedis("192.168.3.168", 6379);
        mRedis.set("key", "hello world");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            mRedis.get("key");
        }
        System.out.println(System.currentTimeMillis() - start);
        mRedis.shutdown();
    }

}
