package com.sdp.db;

/**
 * Created by Guoqing on 2016/4/25.
 */
public interface DBClientInterface {

    void setDB(String host, int port);

    boolean set(String key, String value);

    String get(String key);
}
