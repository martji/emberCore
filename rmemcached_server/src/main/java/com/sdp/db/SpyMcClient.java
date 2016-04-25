package com.sdp.db;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by Guoqing on 2016/4/25.
 */
public class SpyMcClient implements DBClientInterface {

    private MemcachedClient mc = null;

    private final int EXPTIME = 60*60*24*10;

    public SpyMcClient() {

    }

    public SpyMcClient(String host, int port) {
        try {
            mc = new MemcachedClient(new InetSocketAddress(host, port));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setDB(String host, int port) {
        try {
            mc = new MemcachedClient(new InetSocketAddress(host, port));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean set(String key, String value) {
        if (mc != null) {
            OperationFuture<Boolean> res = mc.set(key, EXPTIME, value);
            try {
                if (res.get()) {
                    return true;
                }
            } catch (Exception e) {}
        }
        return false;
    }

    public String get(String key) {
        if (mc == null) {
            return null;
        }
        return (String) mc.get(key);
    }
}
