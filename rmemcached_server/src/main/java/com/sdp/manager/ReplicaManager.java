package com.sdp.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sdp.common.EMSGID;
import com.sdp.config.GlobalConfigMgr;
import com.sdp.example.Log;
import com.sdp.messageBody.CtsMsg;
import com.sdp.netty.NetMsg;
import com.sdp.replicas.DealHotSpotInterface;
import com.sdp.replicas.LocalSpots;
import com.sdp.server.MServer;
import com.sdp.server.ServerNode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.*;

/**
 * Created by magq on 16/7/6.
 */
public class ReplicaManager implements DealHotSpotInterface, Runnable {

    private final int REPLICA_MODE = 0;
    private final int EXPIRE_TIME = 60*60*24*10;
    private final int UPDATE_STATUS_TIME = 5*1000;
    private final int BUFFER_SIZE = 10;

    /**
     * Local reference, mServer connect with other server and monitor, while
     * memcachedClient connect to local memcached server. The whole server information of cluster.
     */
    private MServer mServer;
    private MemcachedClient memcachedClient;
    private int serverId;
    private Map<Integer, ServerNode> serversMap;

    /**
     * Client channel, use to push replica info to clients.
     */
    private ConcurrentHashMap<Integer, Channel> clientChannelMap;

    /**
     * Map of the replica location of all hot spots, this will bring some memory overhead.
     */
    private ConcurrentHashMap<String, Vector<Integer>> replicasIdMap;

    /**
     * The map of connection to other memcached servers.
     */
    private ConcurrentHashMap<Integer, MemcachedClient> spyClientMap;

    /**
     * These variables are maintained by itself, where serverInfoList is the current
     * status of all servers, hotSpotList is the hot spot distribution, bufferHotSpot is
     * a list of hot spots need replica.
     */
    private List<Map.Entry<Integer, Double>> serverInfoList;
    private ConcurrentHashMap<Integer, Integer> hotSpotsList;
    private Set<String> bufferHotSpots;

    public ReplicaManager() {
        init();
    }

    public void init() {
        this.replicasIdMap = new ConcurrentHashMap<String, Vector<Integer>>();
        this.spyClientMap = new ConcurrentHashMap<Integer, MemcachedClient>();
        this.serverInfoList = new LinkedList<Map.Entry<Integer, Double>>();
        this.hotSpotsList = new ConcurrentHashMap<Integer, Integer>();
        this.bufferHotSpots = new HashSet<String>();
    }

    public void setClientChannelMap(ConcurrentHashMap<Integer, Channel> clientChannelMap) {
        this.clientChannelMap = clientChannelMap;
    }

    public void initLocalReference(MServer server, MemcachedClient client,
                                   ConcurrentHashMap<String, Vector<Integer>> replicasIdMap,
                                   ConcurrentHashMap<Integer, MemcachedClient> spyClientMap) {
        this.mServer = server;
        this.memcachedClient = client;
        this.replicasIdMap = replicasIdMap;
        this.spyClientMap = spyClientMap;

        this.serverId = GlobalConfigMgr.id;
        this.serversMap = GlobalConfigMgr.serversMap;
    }

    public void run() {
        try {
            updateServersInfo();
            Thread.sleep(UPDATE_STATUS_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void dealHotData() {
        Set<String> hotSpots = new HashSet<String>(bufferHotSpots);
        bufferHotSpots.clear();

        Set<String> handledHotSpots = new HashSet<String>();
        Map<String, Integer> hotItems = new HashMap<String, Integer>();

        for (String key : hotSpots) {
            int replicaId = getReplicaId(serverInfoList, key);
            if (replicaId != -1) {
                boolean result = createReplica(key, replicaId);
                if (result) {
                    handledHotSpots.add(key);
                    Vector<Integer> vector = null;
                    if (!replicasIdMap.containsKey(key)) {
                        vector = new Vector<Integer>();
                        vector.add(serverId);
                        vector.add(replicaId);
                        replicasIdMap.put(key, vector);
                    } else {
                        if (!replicasIdMap.get(key).contains(replicaId)) {
                            replicasIdMap.get(key).add(replicaId);
                            vector = replicasIdMap.get(key);
                        }
                    }
                    hotItems.put(key, encodeReplicasInfo(vector));

                    int localCount = vector.size() - 1;
                    updateReplicaDistribute(localCount);
                }
            }
        }
        Log.log.info("[PId: " + Log.id + "] new hot spots: " + handledHotSpots.size() +
                " [create] " + hotSpotsList.toString());
        infoAllClient(hotItems);
    }

    public void dealColdData() {
        if (LocalSpots.coldspots.keySet().size() > 0) {
            Set<String> coldSpots = new HashSet<String>(LocalSpots.coldspots.keySet());
            Map<String, Integer> coldItems = new HashMap<String, Integer>();

            for (String key : coldSpots) {
                int localCount = replicasIdMap.get(key).size() - 1;
                hotSpotsList.put(localCount, hotSpotsList.get(localCount) - 1);
                if (localCount - 1 >= 1) {
                    hotSpotsList.put(localCount - 1, hotSpotsList.get(localCount - 1) + 1);
                }
                int replicaId = replicasIdMap.get(key).size() - 1;
                replicasIdMap.get(key).remove(replicaId);
                coldItems.put(key, encodeReplicasInfo(replicasIdMap.get(key)));
                if (replicasIdMap.get(key).size() == 1) {
                    replicasIdMap.remove(key);
                }
            }
            Log.log.info("[PId: " + Log.id + "] new cold spots: " + coldSpots.size() +
                    " [retire] " + hotSpotsList.toString());
            infoAllClient(coldItems);
            LocalSpots.coldspots = new ConcurrentHashMap<String, String>();
        }
    }

    public void dealHotData(String key) {
        bufferHotSpots.add(key);
        if (bufferHotSpots.size() > BUFFER_SIZE) {
            dealHotData();
        }
    }

    public void handleReadFailed(Channel channel, String key, int failedServerId) {
        String oriKey = MessageManager.getOriKey(key);
        if (replicasIdMap.containsKey(oriKey)) {
            String value;
            if (failedServerId == serverId) {
                Vector<Integer> vector = replicasIdMap.get(oriKey);
                MemcachedClient mClient = spyClientMap.get(vector.get(0));
                value = (String) mClient.get(oriKey);
                if (value != null && value.length() > 0) {
                    memcachedClient.set(oriKey, EXPIRE_TIME, value);
                }

            } else {
                value = (String) memcachedClient.get(oriKey);
                if (value != null && value.length() > 0) {
                    MemcachedClient mClient = spyClientMap.get(failedServerId);
                    mClient.set(oriKey, EXPIRE_TIME, value);
                }
            }
            CtsMsg.nr_read_res.Builder builder = CtsMsg.nr_read_res.newBuilder();
            builder.setKey(key);
            builder.setValue(value);
            NetMsg msg = NetMsg.newMessage();
            msg.setMessageLite(builder);
            msg.setMsgID(EMSGID.nr_read_res);
            channel.write(msg);
        }
    }

    /**
     * Update servers information, this method is called each period.
     */
    private void updateServersInfo() {
        String replicasInfo = mServer.getAReplica();
        if (replicasInfo == null || replicasInfo.length() == 0) {
            return;
        }
        serverInfoList = getServersInfoMap(replicasInfo);
    }

    /**
     * Update replicas distribution.
     * @param localCount
     */
    private void updateReplicaDistribute(int localCount) {
        if (localCount <= 1) {
            hotSpotsList.put(1, hotSpotsList.get(1) + 1);
        } else if (hotSpotsList.containsKey(localCount - 1)) {
            hotSpotsList.put(localCount - 1, hotSpotsList.get(localCount - 1) - 1);
            if (!hotSpotsList.containsKey(localCount)) {
                hotSpotsList.put(localCount, 0);
            }
            hotSpotsList.put(localCount, hotSpotsList.get(localCount) + 1);
        }
    }

    /**
     *
     * @param replicasInfo
     * @return list of server workload status.
     */
    private List<Map.Entry<Integer, Double>> getServersInfoMap(String replicasInfo) {
        List<Map.Entry<Integer, Double>> list = null;
        if (replicasInfo == null || replicasInfo.length() == 0) {
            return list;
        }
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        Map<Integer, Double> cpuCostMap = gson.fromJson(replicasInfo,
                new TypeToken<Map<Integer, Double>>() {}.getType());
        list = new ArrayList<Map.Entry<Integer, Double>>(cpuCostMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
            public int compare(Map.Entry<Integer, Double> mapping1,
                               Map.Entry<Integer, Double> mapping2) {
                return mapping1.getValue().compareTo(mapping2.getValue());
            }
        });
        return list;
    }

    /**
     *
     * @param list
     * @param key
     * @return the location serverId where can a replica be created on for key.
     */
    private int getReplicaId(List<Map.Entry<Integer, Double>> list, String key) {
        if (REPLICA_MODE == 0) {
            return getReplicaIdStrict(list, key);
        } else {
            return getReplicaIdRandomly(list, key);
        }
    }

    private int getReplicaIdStrict(List<Map.Entry<Integer, Double>> list, String key) {
        int replicaId = -1;
        HashSet<String> hosts = new HashSet<String>();
        HashSet<Integer> currentReplicas = new HashSet<Integer>();
        if (replicasIdMap.containsKey(key)) {
            currentReplicas = new HashSet<Integer>(replicasIdMap.get(key));
            for (int id : currentReplicas) {
                hosts.add(serversMap.get(id).getHost());
            }
        } else {
            currentReplicas.add(serverId);
            hosts.add(serversMap.get(serverId).getHost());
        }

        for (int i = 0; i < list.size(); i++) {
            int tmp = list.get(i).getKey();
            if (!currentReplicas.contains(tmp)) {
                if (!hosts.contains(serversMap.get(tmp).getHost())) {
                    return tmp;
                } else if (replicaId == -1) {
                    replicaId = tmp;
                }
            }
        }
        if (replicaId != -1 && replicaId != list.size() - 1) {
            Map.Entry<Integer, Double> tmp = list.get(replicaId);
            list.set(replicaId, list.get(list.size() - 1));
            list.set(list.size() - 1, tmp);
        }
        return replicaId;
    }

    public int getReplicaIdRandomly(List<Map.Entry<Integer, Double>> list, String key) {
        int replicaId = -1;
        HashSet<Integer> currentReplicas = new HashSet<Integer>();
        if (replicasIdMap.containsKey(key)) {
            currentReplicas = new HashSet<Integer>(replicasIdMap.get(key));
        } else {
            currentReplicas.add(serverId);
        }
        if (currentReplicas.size() == list.size()) {
            return -1;
        }

        int len = list.size();
        Random random = new Random();
        int tmp = random.nextInt(len);
        tmp = list.get(tmp).getKey();
        if (!currentReplicas.contains(tmp)) {
            replicaId = tmp;
        }

        return replicaId;
    }

    /**
     *
     * @param key
     * @param replicaId : the location serverId
     * @return whether the replica create succeed.
     */
    public boolean createReplica(String key, int replicaId) {
        MemcachedClient replicaClient;
        if (spyClientMap.containsKey(replicaId)) {
            replicaClient = spyClientMap.get(replicaId);
        } else {
            replicaClient = MessageManager.buildAMClient(replicaId);
            if (replicaClient != null) {
                spyClientMap.put(replicaId, replicaClient);
            } else {
                return false;
            }
        }

        String value = (String) memcachedClient.get(key);
        if (value == null || value.length() == 0) {
            System.out.println("[ERROR] no value fo this key: " + key);
            return false;
        }
        OperationFuture<Boolean> out = replicaClient.set(key, EXPIRE_TIME, value);
        try {
            return out.get();
        } catch (Exception e) {}
        return false;
    }

    public int encodeReplicasInfo(Vector<Integer> replicas) {
        int result = 0;
        for (int id : replicas) {
            result += Math.pow(2, id);
        }
        return result;
    }

    /**
     * Push the replica information to clients.
     * @param hotItems
     */
    private void infoAllClient(Map<String, Integer> hotItems) {
        if (hotItems == null || hotItems.size() == 0) {
            return;
        }
        Collection<Channel> clients = clientChannelMap.values();
        Vector<Channel> tmp = new Vector<Channel>();
        tmp.addAll(clients);
        for (Channel channel: tmp) {
            if (! channel.isConnected()) {
                clients.remove(channel);
            }
        }

        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        String replicasInfo = gson.toJson(hotItems);
        CtsMsg.nr_replicas_res.Builder builder = CtsMsg.nr_replicas_res.newBuilder();
        builder.setKey("");
        builder.setValue(replicasInfo);
        NetMsg msg = NetMsg.newMessage();
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nr_replicas_res);
        for (Channel channel: clients) {
            channel.write(msg);
        }
    }
}
