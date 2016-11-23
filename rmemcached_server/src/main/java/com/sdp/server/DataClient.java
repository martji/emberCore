package com.sdp.server;

/**
 * Created by Guoqing on 2016/11/21.
 * Ember server is an universal middle ware for distributed memory stores(redis, memcached, etc.). Before use
 * ember server, DataClient must be implemented.
 */
public interface DataClient {

    String get(String key);

    boolean set(String key, String value);
}
