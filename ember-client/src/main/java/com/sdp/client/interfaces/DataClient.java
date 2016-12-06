package com.sdp.client.interfaces;

/**
 * @author martji
 *
 * DataClient includes {@link com.sdp.client.memcached.McDataClient}, {@link com.sdp.client.redis.RedisDataClient} and
 * {@link com.sdp.client.ember.EmberDataClient}
 */

public interface DataClient {

    void init();

    void shutdown();

    String get(String key);

    boolean set(String key, String value);

    boolean delete(String key);
}
