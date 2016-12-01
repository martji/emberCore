package com.sdp.client.interfaces;

/**
 * @author martji
 */

public interface DataClient {

    void init();

    void shutdown();

    String get(String key);

    boolean set(String key, String value);

    boolean delete(String key);
}
