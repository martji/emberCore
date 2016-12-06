package com.sdp.client;

import com.sdp.client.ember.EmberDataClient;
import com.sdp.client.interfaces.DataClient;
import com.sdp.server.ServerNode;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Guoqing on 2016/11/25.
 */
public class DBClient implements DataClient {

    private int recordCount;
    private int dataHashMode;
    private int dataSetMode;

    public static final int SLICE_HASH_MODE = 0;
    public static final int INDEX_HASH_MODE = 1;
    public static final int RANDOM_HASH_MODE = 2;

    public static final int SYNC_SET_MODE = 0;
    public static final int ASYNC_SET_MODE = 0;

    private int clientType;
    private Map<Integer, DataClient> clientMap;
    private List<Integer> clientIdList;
    private ConcurrentHashMap<String, Vector<Integer>> replicaTable;

    public DBClient(int clientType, List<ServerNode> nodes) {
        this.clientType = clientType;
        this.clientMap = new HashMap<Integer, DataClient>();
        this.clientIdList = new ArrayList<Integer>();
        this.replicaTable = new ConcurrentHashMap<String, Vector<Integer>>();
        for (ServerNode node : nodes) {
            DataClient client = DataClientFactory.createInstance(clientType, node, replicaTable);
            clientMap.put(node.getId(), client);
            clientIdList.add(node.getId());
        }
    }

    public void initConfig(int recordCount, int dataHashMode, int dataSetMode) {
        this.recordCount = recordCount;
        this.dataHashMode = dataHashMode;
        this.dataSetMode = dataSetMode;
    }

    public void init() {

    }

    public void shutdown() {
        Collection<DataClient> clients = clientMap.values();
        for (DataClient client : clients) {
            client.shutdown();
        }
    }

    public String get(String key) {
        String value;
        int masterId = getDataLocation(key);
        if (clientType == DataClientFactory.EMBER_MODE) {
            if (replicaTable.containsKey(key)) {
                int replicaIndex = getOneReplica(key);
                int replicaId = replicaTable.get(key).get(replicaIndex);
                EmberDataClient client = (EmberDataClient) clientMap.get(replicaId);
                value = client.get(key, masterId == replicaId);
                if (value == null) {
                    removeOneReplica(key, replicaIndex);
//                client.asyncGetFromEmber(key, replicaId);
                }
            } else {
                EmberDataClient client = (EmberDataClient) clientMap.get(masterId);
                value = client.get(key, true);
            }
        } else {
            value = clientMap.get(masterId).get(key);
        }
        return value;
    }

    public boolean set(String key, String value) {
        boolean result;
        int masterId = getDataLocation(key);
        if (clientType == DataClientFactory.EMBER_MODE) {
            EmberDataClient client = (EmberDataClient) clientMap.get(masterId);
            result = client.set(key, value);
        } else {
            result = clientMap.get(masterId).set(key, value);
        }
        return result;
    }

    public boolean delete(String key) {
        return false;
    }

    /**
     * @param key
     * @return location of the request key
     */
    public int getDataLocation(String key) {
        try {
            int clientNum = clientIdList.size();
            if (clientNum != 1) {
                if (dataHashMode == SLICE_HASH_MODE) {
                    return getSliceMem(key, clientNum);
                } else if (dataHashMode == INDEX_HASH_MODE) {
                    return getIndexMem(key, clientNum);
                } else if (dataHashMode == RANDOM_HASH_MODE) {
                    return getRandomMem(key, clientNum);
                }
            }
        } catch (Exception e) {
        }
        return clientIdList.get(0);
    }

    public int getSliceMem(String key, int clientNum) {
        int index = key.lastIndexOf("user") + 4;
        int keyNum = Integer.decode(key.substring(index));
        int gap = recordCount / clientNum;
        int leaderIndex = keyNum / gap;
        leaderIndex = Math.abs(leaderIndex);
        return clientIdList.get(leaderIndex);
    }

    public int getIndexMem(String key, int clientNum) {
        int index = key.lastIndexOf("user") + 4;
        int keyNum = Integer.decode(key.substring(index));
        int leaderIndex = keyNum % clientNum;
        leaderIndex = Math.abs(leaderIndex);
        return clientIdList.get(leaderIndex);
    }

    public int getRandomMem(String key, int clientNum) {
        int leaderIndex = key.hashCode() % clientNum;
        leaderIndex = Math.abs(leaderIndex);
        return clientIdList.get(leaderIndex);
    }

    /**
     * @param key
     * @return the index of replicaId, not replicaId
     */
    public int getOneReplica(String key) {
        Vector<Integer> vector = replicaTable.get(key);
        int index = new Random().nextInt(vector.size());
        return index;
    }

    public void removeOneReplica(String key, int replicasId) {
        Vector<Integer> vector = replicaTable.get(key);
        if (vector.size() > 1) {
            vector.remove(replicasId);
        }
    }
}
