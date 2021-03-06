package com.sdp.client;

import com.sdp.client.ember.EmberDataClient;
import com.sdp.client.ember.EmberSetThread;
import com.sdp.client.interfaces.DataClient;
import com.sdp.client.reedsolomon.RSDataClient;
import com.sdp.client.reedsolomon.RSGetThread;
import com.sdp.client.reedsolomon.RSSetThread;
import com.sdp.log.Log;
import com.sdp.server.ServerNode;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by Guoqing on 2016/11/25.
 */
public class DBClient implements DataClient {

    private final int TIMEOUT = 2500;

    private int recordCount;
    private int dataHashMode;
    private int dataSetMode;

    public static final int SLICE_HASH_MODE = 0;
    public static final int INDEX_HASH_MODE = 1;
    public static final int RANDOM_HASH_MODE = 2;

    public static final int SYNC_SET_MODE = 0;
    public static final int ASYNC_SET_MODE = 0;

    private int clientType;
    private int replicaMode;
    private List<ServerNode> serverNodes;
    private Map<Integer, DataClient> clientMap;
    private List<Integer> clientIdList;
    private ConcurrentHashMap<String, Vector<Integer>> replicaTable;

    private int dataShards;
    private int parityShards;

    private int replicasNum;

    private ExecutorService threadPool = Executors.newFixedThreadPool(16);

    public DBClient(int clientType, int replicaMode, List<ServerNode> serverNodes, Properties p) {
        this.clientType = clientType;
        this.replicaMode = replicaMode;
        this.serverNodes = serverNodes;
        this.clientMap = new HashMap<Integer, DataClient>();
        this.clientIdList = new ArrayList<Integer>();
        this.replicaTable = new ConcurrentHashMap<String, Vector<Integer>>();
        for (ServerNode node : this.serverNodes) {
            DataClient client = DataClientFactory.createInstance(clientType, replicaMode, node, replicaTable);
            clientMap.put(node.getId(), client);
            clientIdList.add(node.getId());
        }

        if (clientType == DataClientFactory.RS_TYPE) {
            dataShards = Integer.decode(p.getProperty("data_shards", "2"));
            parityShards = Integer.decode(p.getProperty("parity_shards", "2"));
            RSDataClient.initReedSolomonUtil(dataShards, parityShards);
        } else if (replicaMode == DataClientFactory.REPLICA_EMBER) {
            replicasNum = Integer.decode(p.getProperty("replicas_num", "0"));
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
        if (clientType == DataClientFactory.EMBER_TYPE) {
            if (replicaTable.containsKey(key)) {
                int replicaIndex = getOneReplica(key);
                int replicaId = replicaTable.get(key).get(replicaIndex);
                EmberDataClient client = (EmberDataClient) clientMap.get(replicaId);
                if (client == null) {
                    value = null;
                    Log.log.error("[DBClient] wrong replica id = " + replicaId);
                } else {
                    value = client.get(key, masterId == replicaId);
                }
                if (value == null) {
                    removeOneReplica(key, replicaIndex);
                    value = clientMap.get(masterId).get(key);
                }
            } else {
                EmberDataClient client = (EmberDataClient) clientMap.get(masterId);
                value = client.get(key, true);
            }
        } else if (clientType == DataClientFactory.RS_TYPE) {
            RSDataClient client = (RSDataClient) clientMap.get(masterId);
            value = rsGet(client, key);
        } else {
            value = clientMap.get(masterId).get(key);
        }
        return value;
    }

    public boolean set(String key, String value) {
        boolean result;
        int masterId = getDataLocation(key);
        if (clientType == DataClientFactory.EMBER_TYPE) {
            if (replicaTable.containsKey(key)) {
                result = emberSet(masterId, key, value);
            } else {
                EmberDataClient client = (EmberDataClient) clientMap.get(masterId);
                result = client.set2DataServer(key, value);
            }
        } else if (clientType == DataClientFactory.RS_TYPE) {
            RSDataClient client = (RSDataClient) clientMap.get(masterId);
            result = rsSet(client, key, value);
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
    private int getDataLocation(String key) {
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
            e.printStackTrace();
        }
        return clientIdList.get(0);
    }

    private int getSliceMem(String key, int clientNum) {
        int index = key.lastIndexOf("user") + 4;
        int keyNum = Integer.decode(key.substring(index));
        int gap = recordCount / clientNum;
        int leaderIndex = keyNum / gap;
        leaderIndex = Math.abs(leaderIndex);
        return clientIdList.get(leaderIndex);
    }

    private int getIndexMem(String key, int clientNum) {
        int index = key.lastIndexOf("user") + 4;
        int keyNum = Integer.decode(key.substring(index));
        int leaderIndex = keyNum % clientNum;
        leaderIndex = Math.abs(leaderIndex);
        return clientIdList.get(leaderIndex);
    }

    private int getRandomMem(String key, int clientNum) {
        int leaderIndex = key.hashCode() % clientNum;
        leaderIndex = Math.abs(leaderIndex);
        return clientIdList.get(leaderIndex);
    }

    /**
     * @param key
     * @return the index of replicaId, not replicaId
     */
    private int getOneReplica(String key) {
        Vector<Integer> vector = replicaTable.get(key);
        return new Random().nextInt(vector.size());
    }

    private void removeOneReplica(String key, int replicasId) {
        Vector<Integer> vector = replicaTable.get(key);
        if (vector.size() > 1) {
            vector.remove(replicasId);
        } else {
            replicaTable.remove(key);
        }
    }

    private boolean emberSet(int masterId, String key, String value) {
        boolean result = false;
        EmberDataClient masterClient = (EmberDataClient) clientMap.get(masterId);
        Vector<Integer> replicas = replicaTable.get(key);
        if (replicaMode == DataClientFactory.REPLICA_SPORE) {
            if (replicas != null && replicas.size() > 0) {
                int replicaId = replicas.firstElement();
                EmberDataClient replicaClient = (EmberDataClient) clientMap.get(replicaId);
                result = masterClient.set2DataServer(key, value) && replicaClient.set2DataServer(key, value);
            }
        } else if (replicaMode == DataClientFactory.REPLICA_EMBER) {
            CountDownLatch latch = new CountDownLatch(replicasNum);
            Vector<Boolean> values = new Vector<Boolean>();
            replicasNum = Math.min(replicasNum, replicas.size() - 1);
            if (replicasNum > 0) {
                for (Integer replicaId : replicas) {
                    if (replicaId != masterId) {
                        EmberSetThread thread = new EmberSetThread(clientMap.get(replicaId), key, value, latch, values);
                        threadPool.submit(thread);
                    }
                }
            }
            try {
                result = masterClient.set2DataServer(key, value);
                latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
                for (int i = 0; i < values.size() && result; i++) {
                    result = values.get(i);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * Set the value to some servers
     *
     * @param client
     * @param key
     * @param value
     * @return
     */
    private boolean rsSet(RSDataClient client, String key, String value) {
        boolean result = false;
        Vector<Integer> vector = client.getServerLocations(getDataLocation(key), serverNodes);
        if (vector == null || vector.size() != dataShards + parityShards) {
            result = false;
        } else {
            String[] values = RSDataClient.encode(value);
            if (values != null) {
                Future<Boolean>[] futures = new Future[vector.size()];
                CountDownLatch latch = new CountDownLatch(vector.size());
                for (int i = 0; i < vector.size(); i++) {
                    RSSetThread thread = new RSSetThread(clientMap.get(vector.get(i)), key, values[i], latch);
                    futures[i] = threadPool.submit(thread);
                }
                try {
                    latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
                    result = true;
                    for (Future<Boolean> future : futures) {
                        if (!future.get()) {
                            result = false;
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    /**
     * Get the values from some servers
     *
     * @param client
     * @param key
     * @return
     */
    private String rsGet(RSDataClient client, String key) {
        Vector<Integer> vector = client.getServerLocations(getDataLocation(key), serverNodes);
        if (vector == null || vector.size() != dataShards + parityShards) {
            return null;
        } else {
            String[] arrValue = new String[dataShards + parityShards];
            CountDownLatch latch = new CountDownLatch(dataShards);
            Vector<String> values = new Vector<>();
            int len = vector.size();
            int index = new Random().nextInt(len);
            for (int i = 0; i < dataShards + 1; i++) {
                RSGetThread thread = new RSGetThread(clientMap.get(vector.get((index + i) % len)), key, latch, values);
                threadPool.submit(thread);
            }
            try {
                latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
                for (int i = 0; i < values.size(); i++) {
                    arrValue[i] = values.get(i);
                }
                return RSDataClient.decode(arrValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
