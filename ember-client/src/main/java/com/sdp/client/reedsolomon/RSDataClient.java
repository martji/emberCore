package com.sdp.client.reedsolomon;

import com.sdp.client.DataClientFactory;
import com.sdp.client.ember.EmberClient;
import com.sdp.client.interfaces.DataClient;
import com.sdp.server.ServerNode;
import com.sdp.utils.ReedSolomonUtil;
import com.sdp.utils.ServerTransUtil;

import java.io.UnsupportedEncodingException;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Guoqing on 2016/12/15.
 * @author OSDI-2016-EC-Cache: Load-Balanced, Low-Latency Cluster Caching with Online Erasure Coding
 */
public class RSDataClient implements DataClient {

    private DataClient dataClient;
    private EmberClient emberClient;

    private int serverType;
    private int replicaMode;

    private ConcurrentHashMap<String, Vector<Integer>> replicaTable;

    private static ReedSolomonUtil reedSolomonUtil;
    private static int totalShards;

    public RSDataClient(int serverType, int replicaMode, ServerNode serverNode,
                           ConcurrentHashMap<String, Vector<Integer>> replicaTable) {
        this.serverType = serverType;
        this.replicaMode = replicaMode;
        this.replicaTable = replicaTable;
        init(serverNode);
    }

    public void init(ServerNode serverNode) {
        dataClient = DataClientFactory.createInstance(serverType, serverNode, replicaTable);
        emberClient = new EmberClient(replicaMode, serverNode, replicaTable);
    }

    public void init() {
    }

    public void shutdown() {
        emberClient.shutdown();
        dataClient.shutdown();
    }

    public String get(String key) {
        return dataClient.get(key);
    }

    public boolean set(String key, String value) {
        return dataClient.set(key, value);
    }

    public boolean delete(String key) {
        return false;
    }

    public static void initReedSolomonUtil(int dataShards, int parityShards) {
        totalShards = dataShards + parityShards;
        reedSolomonUtil = new ReedSolomonUtil(dataShards, parityShards);
    }

    public static String[] encode(String value) {
        try {
            return reedSolomonUtil.encode(value);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String decode(String[] arrStr) {
        try {
            return reedSolomonUtil.decode(arrStr);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Vector<Integer> getServerLocations(String key) {
        String result = emberClient.getRSServers(key, totalShards);
        if (result == null) {
            return null;
        }
        int value = Integer.valueOf(result);
        return ServerTransUtil.decodeServer(value);
    }
}
