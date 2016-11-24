package com.sdp.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sdp.common.EMSGID;
import com.sdp.config.ConfigManager;
import com.sdp.log.Log;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.messagebody.CtsMsg;
import com.sdp.netty.NetMsg;
import com.sdp.replicas.LocalSpots;
import com.sdp.server.DataClient;
import com.sdp.server.EmberServer;
import com.sdp.server.EmberServerNode;
import com.sdp.server.dataclient.DataClientFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by magq on 16/7/6.
 */
public class ReplicaManager implements DealHotSpotInterface, Runnable {

    private final int SPORE_MODE = 1;
    private final int EMBER_MODE = 0;

    /**
     * The replica mode.
     */
    private int replicaMode;
    private int updateStatusTime;
    private int bufferSize;
    private int serverId;
    private Map<Integer, EmberServerNode> serversMap;

    /**
     * Local reference, mServer connect with other ember servers and monitor.
     */
    private EmberServer mServer;

    public void setServer(EmberServer mServer) {
        this.mServer = mServer;
    }

    private DataClient mClient;

    /**
     * Client channel, use to push replica info to clients.
     */
    private ConcurrentHashMap<Integer, Channel> clientChannelMap;

    /**
     * Map of the replica location of all hot spots, this will bring some memory overhead.
     */
    private ConcurrentHashMap<String, Vector<Integer>> replicasIdMap;

    /**
     * The map of connection to other data servers.
     */
    private ConcurrentHashMap<Integer, DataClient> dataClientMap;

    /**
     * These variables are maintained by itself, where serverInfoList is the current
     * status of all servers, hotSpotList is the hot spot distribution, bufferHotSpot is
     * a list of hot spots need replica.
     */
    private List<Map.Entry<Integer, Double>> serverInfoList;
    private ConcurrentHashMap<Integer, Integer> replicasDistribute;
    private ConcurrentLinkedQueue<String> bufferHotSpots;

    /**
     * The percentage of handled hot spots.
     */
    private double handledPercentage;

    private ExecutorService threadPool;
    private ExecutorService retireThread;

    public ReplicaManager() {
        init();
    }

    public void init() {
        initConfig();

        this.replicasIdMap = new ConcurrentHashMap<String, Vector<Integer>>();
        this.dataClientMap = new ConcurrentHashMap<Integer, DataClient>();
        this.serverInfoList = new LinkedList<Map.Entry<Integer, Double>>();
        this.replicasDistribute = new ConcurrentHashMap<Integer, Integer>();
        replicasDistribute.put(1, 0);
        this.bufferHotSpots = new ConcurrentLinkedQueue<String>();
        this.threadPool = Executors.newCachedThreadPool();
        this.retireThread = Executors.newSingleThreadExecutor();
    }

    public void initConfig() {
        this.serverId = ConfigManager.id;
        this.serversMap = ConfigManager.serversMap;
        this.replicaMode = (Integer) ConfigManager.propertiesMap.get(ConfigManager.REPLICA_MODE);
        this.updateStatusTime = (Integer) ConfigManager.propertiesMap.get(ConfigManager.UPDATE_STATUS_TIME);
        this.bufferSize = (Integer) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_BUFFER_SIZE);
    }

    public void setClientChannelMap(ConcurrentHashMap<Integer, Channel> clientChannelMap) {
        this.clientChannelMap = clientChannelMap;
    }

    public void initLocalReference(EmberServer server, ConcurrentHashMap<String, Vector<Integer>> replicasIdMap) {
        this.mServer = server;
        this.replicasIdMap = replicasIdMap;
        this.dataClientMap = server.getDataClientMap();
        this.mClient = dataClientMap.get(serverId);
    }

    public void run() {
        try {
            while (true) {
                updateServersInfo();
                Thread.sleep(updateStatusTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Update servers information, this method is called each period.
     */
    private void updateServersInfo() {
        if (mServer != null) {
            String replicasInfo = mServer.getAReplica();
            if (replicasInfo == null || replicasInfo.length() == 0) {
                return;
            }
            serverInfoList = getServersInfoMap(replicasInfo);
        }
    }

    public void dealHotData() {
        if (bufferHotSpots.size() == 0) {
            return;
        }

        final LinkedList<String> hotSpots = new LinkedList<String>(bufferHotSpots);
        bufferHotSpots.clear();
        threadPool.execute(new Runnable() {
            public void run() {
                dealHotData(hotSpots);
            }
        });
    }

    /**
     * Deal a single hot spot, the hot spot will not be dealt at once, and the hot spot will
     * be inset to a buffer. If the buffer size reaches the predefined bufferSize, @method dealHotData()
     * will be invoked.
     * @param key
     */
    public void dealHotData(String key) {
        bufferHotSpots.add(key);
        if (bufferHotSpots.size() > bufferSize) {
            dealHotData();
        }
    }

    /**
     * Deal the hot spot in buffer and create replicas.
     */
    public void dealHotData(LinkedList<String> hotSpots) {
        if (hotSpots.size() == 0) {
            handledPercentage = 1;
            return;
        }
        int id = (int) (Math.random() * bufferSize);
        Log.log.info(id + " [hot spot] new hot spots number = " + hotSpots.size());

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
                    } else if (!replicasIdMap.get(key).contains(replicaId)) {
                        replicasIdMap.get(key).add(replicaId);
                        vector = replicasIdMap.get(key);
                    }
                    hotItems.put(key, encodeReplicasInfo(vector));

                    int replicasNum = vector.size() - 1;
                    updateReplicaDistribute(replicasNum);
                }
            }
        }
        handledPercentage = (double) handledHotSpots.size() / hotSpots.size();

        Log.log.info(id + " [hot spots] handled number = " + handledHotSpots.size() +
                ", handled percentage =  " + handledPercentage +
                " | current distribution = " + replicasDistribute.toString());
        infoAllClient(hotItems);
    }

    public void dealColdData() {
        HashSet<String> candidates = null;
        if (replicaMode == SPORE_MODE) {
            candidates = new HashSet<String>(replicasIdMap.keySet());
        } else if (LocalSpots.candidateColdSpots.size() > 0) {
            candidates = new HashSet<String>(LocalSpots.candidateColdSpots.keySet());
        }
        if (candidates != null && candidates.size() > 0) {
            Log.log.info("[cold spots] number = " + candidates.size());
            final int hotSpotNumber = LocalSpots.hotSpotNumber.get();
            final HashSet<String> finalCandidates = candidates;
            retireThread.execute(new Runnable() {
                public void run() {
                    dealColdData(finalCandidates, hotSpotNumber);
                }
            });
        }
        LocalSpots.candidateColdSpots = new ConcurrentHashMap<String, Object>(replicasIdMap);
        LocalSpots.hotSpotNumber.set(0);
    }

    public void dealColdData(HashSet<String> coldSpots, int hotSpotNumber) {
        int id = (int) (Math.random() * bufferSize);
        Log.log.info(id + " [cold spots] start deal cold spots = " + coldSpots.size());
        Map<String, Integer> coldItems = new HashMap<String, Integer>();

        for (String key : coldSpots) {
            if (replicasIdMap.containsKey(key)) {
                int localCount = replicasIdMap.get(key).size() - 1;
                replicasDistribute.put(localCount, replicasDistribute.get(localCount) - 1);
                if (localCount - 1 >= 1) {
                    replicasDistribute.put(localCount - 1, replicasDistribute.get(localCount - 1) + 1);
                }
                int replicaIdIndex = replicasIdMap.get(key).size() - 1;
                replicasIdMap.get(key).remove(replicaIdIndex);
                coldItems.put(key, encodeReplicasInfo(replicasIdMap.get(key)));
                if (replicasIdMap.get(key).size() == 1) {
                    replicasIdMap.remove(key);
                }
            }
        }
        Log.log.info(id + " [cold spots] new cold spots = " + coldSpots.size() + "/" + replicasIdMap.size() +
                ", cold spots/hot spots = " + (double) coldSpots.size() / hotSpotNumber +
                " | current distribution = " + replicasDistribute.toString());
        infoAllClient(coldItems);
    }

    /**
     * Handle read failed from other server.
     * @param channel
     * @param key
     * @param failedServerId
     */
    public void handleReadFailed(Channel channel, String key, int failedServerId) {
        String oriKey = MessageManager.getOriKey(key);
        if (replicasIdMap.containsKey(oriKey)) {
            String value;
            if (failedServerId == serverId) {
                Vector<Integer> vector = replicasIdMap.get(oriKey);
                DataClient mClient = dataClientMap.get(vector.get(0));
                value = mClient.get(oriKey);
                if (value != null && value.length() > 0) {
                    this.mClient.set(oriKey, value);
                }

            } else {
                value = mClient.get(oriKey);
                if (value != null && value.length() > 0) {
                    DataClient mClient = dataClientMap.get(failedServerId);
                    mClient.set(oriKey, value);
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
     * Update replicas distribution.
     * @param replicasNum
     */
    private void updateReplicaDistribute(int replicasNum) {
        if (replicasNum <= 1) {
            replicasDistribute.put(1, replicasDistribute.get(1) + 1);
        } else if (replicasDistribute.containsKey(replicasNum - 1)) {
            replicasDistribute.put(replicasNum - 1, replicasDistribute.get(replicasNum - 1) - 1);
            if (!replicasDistribute.containsKey(replicasNum)) {
                replicasDistribute.put(replicasNum, 0);
            }
            replicasDistribute.put(replicasNum, replicasDistribute.get(replicasNum) + 1);
        }
    }

    /**
     * Transfer replicasInfo to list.
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
        if (replicaMode == EMBER_MODE) {
            return getReplicaIdStrict(list, key);
        } else {
            return getReplicaIdRandom(list, key);
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

    public int getReplicaIdRandom(List<Map.Entry<Integer, Double>> list, String key) {
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
        DataClient replicaClient;
        if (dataClientMap.containsKey(replicaId)) {
            replicaClient = dataClientMap.get(replicaId);
        } else {
            replicaClient = DataClientFactory.createDataClient(replicaId);
            if (replicaClient != null) {
                dataClientMap.put(replicaId, replicaClient);
            } else {
                return false;
            }
        }

        String value = mClient.get(key);
        if (value == null || value.length() == 0) {
            Log.log.error("[ERROR] no value fo this key: " + key);
            return false;
        }
        return replicaClient.set(key, value);
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
