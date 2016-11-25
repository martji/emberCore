package com.sdp.client.redis;

import com.sdp.client.interfaces.DataClient;
import com.sdp.server.ServerNode;
import redis.clients.jedis.Jedis;

/**
 * Created by Guoqing on 2016/4/19.
 */
public class RedisDataClient implements DataClient {

    private Jedis jedis;

    public RedisDataClient(String host, int port) {
        try {
            jedis = new Jedis(host, port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public RedisDataClient(ServerNode node) {
        this(node.getHost(), node.getDataPort());
    }

    public void init() {

    }

    public void shutdown() {
        jedis.close();
    }

    public String get(String key) {
        return jedis.get(key);
    }

    public boolean set(String key, String value) {
        String out = jedis.set(key, value);
        return out != null;
    }

    public boolean delete(String key) {
        return false;
    }

    public static void main(String[] args) {
        RedisDataClient mRedis = new RedisDataClient("192.168.3.168", 6379);
        mRedis.set("key", "hello world");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            mRedis.get("key");
        }
        System.out.println(System.currentTimeMillis() - start);
        mRedis.shutdown();
    }
}
