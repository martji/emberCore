package com.sdp.client;

import com.sdp.client.ember.EmberDataClient;
import com.sdp.client.interfaces.DataClient;
import com.sdp.client.memcached.McDataClient;
import com.sdp.client.redis.RedisDataClient;
import com.sdp.server.ServerNode;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Guoqing on 2016/11/25.
 */
public class DataClientFactory {

    public static final int MC_MODE = 0;
    public static final int REDIS_MODE = 1;
    public static final int EMBER_MODE = 2;

    public static final int REPLICA_SPORE = 1;
    public static final int REPLICA_EMBER = 0;

    public static DataClient createInstance(int type, ServerNode node, ConcurrentHashMap<String, Vector<Integer>> replicaTable) {
        return createInstance(type, REPLICA_EMBER, node, replicaTable);
    }

    public static DataClient createInstance(int type, int replicaMode, ServerNode node, ConcurrentHashMap<String, Vector<Integer>> replicaTable) {
        switch (type) {
            case MC_MODE:
                return new McDataClient(node);
            case REDIS_MODE:
                return new RedisDataClient(node);
            case EMBER_MODE:
                return new EmberDataClient(MC_MODE, replicaMode, node, replicaTable);
        }
        return null;
    }
}
