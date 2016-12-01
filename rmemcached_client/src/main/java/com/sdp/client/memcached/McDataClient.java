package com.sdp.client.memcached;

import com.sdp.client.interfaces.DataClient;
import com.sdp.server.ServerNode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by Guoqing on 2016/11/25.
 */
public class McDataClient implements DataClient {

    private int EXPIRE_TIME = 60 * 60 * 24 * 10;

    private MemcachedClient mc;

    public McDataClient(String host, int port) {
        try {
            mc = new MemcachedClient(new InetSocketAddress(host, port));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public McDataClient(ServerNode node) {
        this(node.getHost(), node.getDataPort());
    }

    public void init() {

    }

    public void shutdown() {
        mc.shutdown();
    }

    public String get(String key) {
        return mc.get(key).toString();
    }

    public boolean set(String key, String value) {
        OperationFuture<Boolean> res = mc.set(key, EXPIRE_TIME, value);
        try {
            return res.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean delete(String key) {
        return false;
    }
}
