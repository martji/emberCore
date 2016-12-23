package com.sdp.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sdp.common.EMSGID;
import com.sdp.config.ConfigManager;
import com.sdp.replicas.StatusItem;
import com.sdp.utils.ConstUtil;
import com.sdp.log.Log;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.message.CtsMsg;
import com.sdp.netty.NetMsg;
import com.sdp.utils.DataUtil;
import com.sdp.utils.SpotUtil;
import com.sdp.server.DataClient;
import com.sdp.server.EmberServer;
import com.sdp.server.EmberServerNode;
import com.sdp.server.dataclient.DataClientFactory;
import org.jboss.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by magq on 16/7/6.
 */
public class ReplicaManager implements DealHotSpotInterface, Runnable {

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
    private ConcurrentHashMap<String, Vector<Integer>> replicaTable;
    private ConcurrentHashMap<String, Integer> replicaBuffer;

    /**
     * The map of connection to other data servers.
     */
    private ConcurrentHashMap<Integer, DataClient> dataClientMap;

    /**
     * These variables are maintained by itself, where serverWorkloadList is the current
     * status of all servers, hotSpotList is the hot spot distribution, bufferHotSpot is
     * a list of hot spots need replica.
     */
    private List<StatusItem> serverWorkloadList;
    private double unbalanceRatio;
    private ConcurrentHashMap<Integer, Integer> replicasDistribute;
    private ConcurrentLinkedQueue<String> hotSpotBuffer;

    private ExecutorService threadPool;

    public ReplicaManager() {
        init();
    }

    public void init() {
        initConfig();

        this.replicaTable = new ConcurrentHashMap<String, Vector<Integer>>();
        this.replicaBuffer = new ConcurrentHashMap<String, Integer>();
        this.dataClientMap = new ConcurrentHashMap<Integer, DataClient>();
        this.serverWorkloadList = new ArrayList<StatusItem>();
        this.replicasDistribute = new ConcurrentHashMap<Integer, Integer>();
        this.replicasDistribute.put(1, 0);
        this.hotSpotBuffer = new ConcurrentLinkedQueue<String>();
        this.threadPool = Executors.newCachedThreadPool();
    }

    public void initConfig() {
        this.serverId = ConfigManager.id;
        this.serversMap = ConfigManager.serversMap;
        this.replicaMode = (Integer) ConfigManager.propertiesMap.get(ConfigManager.REPLICA_MODE);
        this.updateStatusTime = (Integer) ConfigManager.propertiesMap.get(ConfigManager.UPDATE_STATUS_TIME);
        this.bufferSize = (Integer) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_BUFFER_SIZE);

        Log.log.info("[ReplicaManager] replicaMode = " + ConstUtil.getReplicaMode(replicaMode) +
                ", bufferSize = " + bufferSize);
    }

    public void setClientChannelMap(ConcurrentHashMap<Integer, Channel> clientChannelMap) {
        this.clientChannelMap = clientChannelMap;
    }

    public void initLocalReference(EmberServer server, ConcurrentHashMap<String, Vector<Integer>> replicaTable) {
        this.mServer = server;
        this.replicaTable = replicaTable;
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
            serverWorkloadList = getServersInfoMap(replicasInfo);
        }
    }

    public void dealHotData() {
        if (hotSpotBuffer.size() > 0) {
            final LinkedList<String> hotSpots = new LinkedList<String>(hotSpotBuffer);
            threadPool.execute(new Runnable() {
                public void run() {
                    dealHotData(hotSpots);
                    syncReplicaTable();
                }
            });
            hotSpotBuffer.clear();
        }
    }

    public void dealColdData() {
        HashSet<String> candidates;
        if (replicaMode == ConstUtil.REPLICA_SPORE) {
            candidates = new HashSet<String>(replicaTable.keySet());
            List<String> keys = new LinkedList<>(SpotUtil.periodHotSpots);
            for (String key : keys) {
                if (candidates.contains(key)) {
                    candidates.remove(key);
                }
            }
        } else {
            candidates = new HashSet<String>(SpotUtil.candidateColdSpots);
        }
        SpotUtil.hotSpotNumber.set(replicaTable.size());
        SpotUtil.coldSpotNumber.set(candidates.size());

        final HashSet<String> coldSpots = candidates;
        threadPool.execute(new Runnable() {
            public void run() {
                if (coldSpots.size() > 0) {
                    dealColdData(coldSpots);
                }
                if (hotSpotBuffer.size() > 0) {
                    List<String> hotSpots = new LinkedList<String>(hotSpotBuffer);
                    dealHotData(hotSpots);
                    hotSpotBuffer.clear();
                }
                syncReplicaTable();
            }
        });
        SpotUtil.reset(replicaTable);
    }

    /**
     * Deal a single hot spot, the hot spot will not be dealt at once, and the hot spot will
     * be inset to a buffer. If the buffer size reaches the predefined bufferSize, dealHotSpot()
     * will be invoked.
     *
     * @param key
     */
    public void dealHotData(String key) {
        SpotUtil.periodHotSpots.add(key);
        hotSpotBuffer.add(key);
        if (hotSpotBuffer.size() > bufferSize) {
            dealHotData();
        }
    }

    public void syncReplicaTable() {
        infoAllClient();
    }

    /**
     * Deal the hot spot in buffer and create replicas.
     */
    public void dealHotData(List<String> hotSpots) {
        if (unbalanceRatio < ConstUtil.UNBALANCE_THRESHOLD) {
            return;
        }
        int id = (int) (Math.random() * bufferSize);
        Log.log.info(String.format("[%d] start deal hotSpots, number = ", id) + hotSpots.size());

        Set<String> handledHotSpots = new HashSet<String>();
        for (String key : hotSpots) {
            int replicaId = getReplicaId(key);
            if (replicaId != -1) {
                boolean result = createReplica(key, replicaId);
                if (result) {
                    handledHotSpots.add(key);
                    Vector<Integer> vector;
                    if (!replicaTable.containsKey(key)) {
                        vector = new Vector<Integer>();
                        vector.add(serverId);
                        vector.add(replicaId);
                        replicaTable.put(key, vector);
                    } else {
                        if (!replicaTable.get(key).contains(replicaId)) {
                            replicaTable.get(key).add(replicaId);
                        }
                        vector = replicaTable.get(key);
                    }
                    replicaBuffer.put(key, encodeReplicasInfo(vector));
                    int replicasNum = vector.size() - 1;
                    updateReplicaDistribute(replicasNum);
                }
            }
        }

        Log.log.info(String.format("[%d] dealt hotSpots = ", id) + handledHotSpots.size() + "/" + hotSpots.size() +
                ", dealt percentage = " + DataUtil.doubleFormat((double) handledHotSpots.size() / hotSpots.size()) +
                " | current distribution = " + replicasDistribute.toString());
    }

    /**
     * Deal the cold spots
     * @param coldSpots
     */
    public void dealColdData(HashSet<String> coldSpots) {
        int id = (int) (Math.random() * bufferSize);
        Log.log.info(String.format("[%d] start deal coldSpots, number = ", id) + coldSpots.size());

        int handledColdSpotNum = 0;
        for (String key : coldSpots) {
            if (replicaTable.containsKey(key)) {
                handledColdSpotNum ++;
                int localCount = replicaTable.get(key).size() - 1;
                replicasDistribute.put(localCount, replicasDistribute.get(localCount) - 1);
                if (localCount - 1 >= 1) {
                    replicasDistribute.put(localCount - 1, replicasDistribute.get(localCount - 1) + 1);
                }
                int replicaIdIndex = replicaTable.get(key).size() - 1;
                replicaTable.get(key).remove(replicaIdIndex);
                replicaBuffer.put(key, encodeReplicasInfo(replicaTable.get(key)));
                if (replicaTable.get(key).size() == 1) {
                    replicaTable.remove(key);
                }
            }
        }

        Log.log.info(String.format("[%d] dealt coldSpots = ", id) + handledColdSpotNum + "/" + coldSpots.size() +
                " | current distribution = " + replicasDistribute.toString());
    }

    /**
     * Handle read failed from other server.
     *
     * @param channel
     * @param key
     * @param failedServerId
     */
    public void handleReadFailed(Channel channel, String key, int failedServerId) {
        String oriKey = MessageManager.getOriKey(key);
        if (replicaTable.containsKey(oriKey)) {
            String value;
            if (failedServerId == serverId) {
                Vector<Integer> vector = replicaTable.get(oriKey);
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

    public void handleRSServers(Channel channel, String key, int count) {
        String oriKey = MessageManager.getOriKey(key);
        Vector<Integer> vector;
        if (replicaTable.containsKey(oriKey)) {
            vector = replicaTable.get(oriKey);
        } else {
            List<Integer> list = new ArrayList<>(serversMap.keySet());
            int index = list.indexOf(serverId);
            vector = new Vector<>();
            for (int i = 0; i < count; i++) {
                vector.add(list.get((index + i) % list.size()));
            }
            replicaTable.put(oriKey, vector);
        }
        String value = encodeReplicasInfo(vector) + "";
        CtsMsg.nr_read_res.Builder builder = CtsMsg.nr_read_res.newBuilder();
        builder.setKey(key);
        builder.setValue(value);
        NetMsg msg = NetMsg.newMessage();
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nr_read_res);
        channel.write(msg);
    }

    /**
     * Update replicas distribution.
     *
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
     *
     * @param replicasInfo
     * @return list of server workload status.
     */
    private List<StatusItem> getServersInfoMap(String replicasInfo) {
        List<StatusItem> list = null;
        if (replicasInfo == null || replicasInfo.length() == 0) {
            return list;
        }
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        Map<Integer, Double> cpuCostMap = gson.fromJson(replicasInfo,
                new TypeToken<Map<Integer, Double>>() {
                }.getType());
        Set<Map.Entry<Integer, Double>> servers = cpuCostMap.entrySet();
        list = new ArrayList<>();
        double localWorkload = 0;
        for (Map.Entry<Integer, Double> entry: servers) {
            list.add(new StatusItem(entry.getKey(), entry.getValue()));
            if (entry.getKey() == serverId) {
                localWorkload = entry.getValue();
            }
        }
        Collections.sort(list);
        if (list.get(0).getWorkload() == 0) {
            unbalanceRatio = ConstUtil.UNBALANCE_THRESHOLD;
        } else {
            unbalanceRatio = localWorkload / list.get(0).getWorkload();
        }
        return list;
    }

    /**
     * @param key
     * @return the location serverId where can a replica be created on for key.
     */
    private int getReplicaId(String key) {
        if (replicaMode == ConstUtil.REPLICA_EMBER) {
            return getReplicaIdStrict(key);
        } else {
            return getReplicaIdRandom(key);
        }
    }

    private int getReplicaIdStrict(String key) {
        int replicaId = -1;
        HashSet<String> hosts = new HashSet<String>();
        HashSet<Integer> currentReplicas = new HashSet<Integer>();
        if (replicaTable.containsKey(key)) {
            currentReplicas = new HashSet<Integer>(replicaTable.get(key));
            for (int id : currentReplicas) {
                hosts.add(serversMap.get(id).getHost());
            }
        } else {
            currentReplicas.add(serverId);
            hosts.add(serversMap.get(serverId).getHost());
        }

        int index = 0;
        int len = serverWorkloadList.size();
        for (int i = 0; i < len; i++) {
            int tmp = serverWorkloadList.get(i).getServerId();
            if (!currentReplicas.contains(tmp)) {
                if (!hosts.contains(serversMap.get(tmp).getHost())) {
                    replicaId = tmp;
                    index = i;
                    break;
                } else if (replicaId == -1) {
                    replicaId = tmp;
                    index = i;
                }
            }
        }
        if (index != 0) {
            StatusItem tmp = serverWorkloadList.get(index);
            serverWorkloadList.set(index, serverWorkloadList.get((index + 1) % len));
            serverWorkloadList.set((index + 1) % len, tmp);
        }
        return replicaId;
    }

    public int getReplicaIdRandom(String key) {
        int replicaId = -1;
        HashSet<Integer> currentReplicas = new HashSet<Integer>();
        if (replicaTable.containsKey(key)) {
            currentReplicas = new HashSet<Integer>(replicaTable.get(key));
        } else {
            currentReplicas.add(serverId);
        }

        if (currentReplicas.size() == serverWorkloadList.size()) {
            return -1;
        }
        int len = serverWorkloadList.size();
        int tmp = new Random().nextInt(len);
        tmp = serverWorkloadList.get(tmp).getServerId();
        if (!currentReplicas.contains(tmp)) {
            replicaId = tmp;
        }
        return replicaId;
    }

    /**
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
     *
     */
    private void infoAllClient() {
        Map<String, Integer> hotItems = new HashMap<>(replicaBuffer);
        replicaBuffer.clear();
        if (hotItems.size() == 0) {
            return;
        }
        Log.log.debug("[ReplicaTable] update number = " + hotItems.size());
        Log.log.trace("[ReplicaTable] update " + hotItems);
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        String replicasInfo = gson.toJson(hotItems);
        CtsMsg.nr_replicas_res.Builder builder = CtsMsg.nr_replicas_res.newBuilder();
        builder.setKey("");
        builder.setValue(replicasInfo);
        NetMsg msg = NetMsg.newMessage();
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nr_replicas_res);

        Collection<Channel> clients = clientChannelMap.values();
        for (Channel channel : clients) {
            if (channel.isConnected()) {
                channel.write(msg);
            }
        }
    }
}
