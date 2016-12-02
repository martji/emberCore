package com.sdp.server.dataclient;

import com.sdp.config.ConfigManager;
import com.sdp.server.DataClient;
import com.sdp.server.EmberServerNode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Created by Guoqing on 2016/11/21.
 * Implement {@link DataClient} with {@link MemcachedClient}.
 */
public class McDataClient implements DataClient {

    private MemcachedClient client;

    private final int EXPIRE_TIME = 60 * 60 * 24 * 10;

    public McDataClient(MemcachedClient client) {
        this.client = client;
    }

    public String get(String key) {
        if (client != null) {
            return (String) client.get(key);
        }
        return null;
    }

    public boolean set(String key, String value) {
        if (client != null) {
            OperationFuture<Boolean> out = client.set(key, EXPIRE_TIME, value);
            try {
                return out.get();
            } catch (Exception e) {
            }
        }
        return false;
    }

    public static McDataClient createInstance(int id) {
        MemcachedClient client = null;
        try {
            Map<Integer, EmberServerNode> serversMap = ConfigManager.serversMap;
            EmberServerNode serverNode = serversMap.get(id);
            String host = serverNode.getHost();
            int port = serverNode.getDataPort();
            client = new MemcachedClient(new InetSocketAddress(host, port));
        } catch (Exception e) {
        }
        return new McDataClient(client);
    }
}
