package com.sdp.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sdp.common.EMSGID;
import com.sdp.config.ConfigManager;
import com.sdp.log.Log;
import com.sdp.manager.hotspotmanager.BaseHotSpotManager;
import com.sdp.manager.hotspotmanager.interfaces.DealHotSpotInterface;
import com.sdp.message.CtsMsg;
import com.sdp.netty.NetMsg;
import com.sdp.replicas.StatusItem;
import com.sdp.server.DataClient;
import com.sdp.server.EmberServer;
import com.sdp.server.EmberServerNode;
import com.sdp.server.dataclient.DataClientFactory;
import com.sdp.utils.ConstUtil;
import com.sdp.utils.DataUtil;
import com.sdp.utils.SpotUtil;
import org.jboss.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by magq on 16/7/6.
 */
public class ReplicaManager implements DealHotSpotInterface, Runnable {

    private static final int DEFAULT_REPLICA_SERVER_ID = -1;

    /**
     * The replica mode.
     */
    private int replicaMode;
    private int updateStatusTime;
    private int hotSpotBufferSize;
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
    private ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> replicaTable;
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
    private ConcurrentHashMap<Integer, ConcurrentSkipListSet<String>> replicaDistribute;
    private ConcurrentLinkedQueue<String> hotSpotBuffer;
    private ConcurrentSkipListSet<String> lastHotSpotSet;

    private ExecutorService threadPool;

    private BaseHotSpotManager hotSpotManager;

    public void setHotSpotManager(BaseHotSpotManager hotSpotManager) {
        this.hotSpotManager = hotSpotManager;
    }

    public ReplicaManager() {
        init();
    }

    private void init() {
        initConfig();

        this.replicaTable = new ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>>();
        this.replicaBuffer = new ConcurrentHashMap<String, Integer>();
        this.dataClientMap = new ConcurrentHashMap<Integer, DataClient>();
        this.serverWorkloadList = new ArrayList<StatusItem>();
        this.replicaDistribute = new ConcurrentHashMap<Integer, ConcurrentSkipListSet<String>>();
        this.replicaDistribute.put(2, new ConcurrentSkipListSet<String>());
        this.hotSpotBuffer = new ConcurrentLinkedQueue<String>();
        this.lastHotSpotSet = new ConcurrentSkipListSet<String>();
        this.threadPool = Executors.newCachedThreadPool();
    }

    public void initConfig() {
        this.serverId = ConfigManager.id;
        this.serversMap = ConfigManager.serversMap;
        this.replicaMode = (Integer) ConfigManager.propertiesMap.get(ConfigManager.REPLICA_MODE);
        this.updateStatusTime = (Integer) ConfigManager.propertiesMap.get(ConfigManager.UPDATE_STATUS_TIME);
        this.hotSpotBufferSize = (Integer) ConfigManager.propertiesMap.get(ConfigManager.HOT_SPOT_BUFFER_SIZE);

        Log.log.info("[ReplicaManager] replicaMode = " + ConstUtil.getReplicaMode(replicaMode) +
                ", hotSpotBufferSize = " + hotSpotBufferSize);
    }

    void setClientChannelMap(ConcurrentHashMap<Integer, Channel> clientChannelMap) {
        this.clientChannelMap = clientChannelMap;
    }

    void initLocalReference(EmberServer server, ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> replicaTable) {
        this.mServer = server;
        this.replicaTable = replicaTable;
        this.dataClientMap = server.getDataClientMap();
        this.mClient = dataClientMap.get(serverId);
    }

    public void run() {
        try {
            while (true) {
                updateWorkloadInfo();
                Thread.sleep(updateStatusTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Update servers workload information, this method is called each period.
     */
    private void updateWorkloadInfo() {
        if (mServer != null) {
            String workloadInfo = mServer.getClusterWorkloadInfo();
            if (workloadInfo == null || workloadInfo.length() == 0) {
                return;
            }
            serverWorkloadList = decodeClusterWorkloadInfo(workloadInfo);
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

        lastHotSpotSet.clear();
        lastHotSpotSet.addAll(SpotUtil.periodHotSpots);
        SpotUtil.reset(replicaTable);
    }

    /**
     * Deal a single hot spot, the hot spot will not be dealt at once, and the hot spot will
     * be inset to a buffer. If the buffer size reaches the predefined hotSpotBufferSize, dealHotSpot()
     * will be invoked.
     *
     * @param key : the hot item
     */
    public void dealHotData(String key) {
        SpotUtil.periodHotSpots.add(key);
        hotSpotBuffer.add(key);
        if (replicaMode == ConstUtil.REPLICA_EMBER && hotSpotBuffer.size() > hotSpotBufferSize) {
            dealHotData();
        }
    }

    /**
     * Deal the hot spot in buffer and create replicas.
     */
    private void dealHotData(List<String> hotSpots) {
        if (unbalanceRatio < ConstUtil.UNBALANCE_THRESHOLD) {
            return;
        }
        int id = (int) (Math.random() * hotSpotBufferSize);
        Log.log.info(String.format("[%d] start deal hotSpots, number = ", id) + hotSpots.size());

        int handledHotSpotNum = 0;
        for (String key : hotSpots) {
            if (replicaMode == ConstUtil.REPLICA_EMBER && lastHotSpotSet.contains(key)) {
                lastHotSpotSet.remove(key);
                if (createMultiReplica(key)) {
                    handledHotSpotNum++;
                    resetHotData(key);
                }
            } else if (createSingleReplica(key)) {
                handledHotSpotNum++;
                resetHotData(key);
            }
        }
        lastHotSpotSet.addAll(hotSpots);

        Log.log.info(String.format("[%d] dealt hotSpots = ", id) + handledHotSpotNum + "/" + hotSpots.size() +
                ", dealt percentage = " + DataUtil.doubleFormat((double) handledHotSpotNum / hotSpots.size()));
    }

    /**
     * Deal the cold spots.
     */
    public void dealColdData(HashSet<String> coldSpots) {
        int id = (int) (Math.random() * hotSpotBufferSize);
        Log.log.info(String.format("[%d] start deal coldSpots, number = ", id) + coldSpots.size());

        int handledColdSpotNum = 0;
        for (String key : coldSpots) {
            if (retireReplica(key)) {
                handledColdSpotNum++;
            }
        }

        Log.log.info(String.format("[%d] dealt coldSpots = ", id) + handledColdSpotNum + "/" + coldSpots.size());
    }

    /**
     * Handle read failed from other server.
     */
    void handleReadFailed(Channel channel, String key, int failedServerId) {
        String oriKey = MessageManager.getOriKey(key);
        if (replicaTable.containsKey(oriKey)) {
            String value;
            if (failedServerId == serverId) {
                Vector<Integer> vector = new Vector<>(replicaTable.get(oriKey));
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
     * Reed-solomon mode to get server locations.
     */
    void handleRSServers(Channel channel, String key, int count) {
        String oriKey = MessageManager.getOriKey(key);
        ConcurrentSkipListSet<Integer> set;
        if (replicaTable.containsKey(oriKey)) {
            set = replicaTable.get(oriKey);
        } else {
            List<Integer> list = new ArrayList<>(serversMap.keySet());
            int index = list.indexOf(serverId);
            set = new ConcurrentSkipListSet<Integer>();
            for (int i = 0; i < count; i++) {
                set.add(list.get((index + i) % list.size()));
            }
            replicaTable.put(oriKey, set);
        }
        String value = encodeReplicasInfo(set) + "";
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
     */
    private void updateReplicaDistribute(String key, int oldReplicasNum, int replicasNum) {
        if (replicasNum != oldReplicasNum) {
            if (oldReplicasNum != 1 && replicaDistribute.containsKey(oldReplicasNum)) {
                replicaDistribute.get(oldReplicasNum).remove(key);
            }

            if (replicasNum > 1) {
                if (!replicaDistribute.containsKey(replicasNum)) {
                    replicaDistribute.put(replicasNum, new ConcurrentSkipListSet<String>());
                }
                replicaDistribute.get(replicasNum).add(key);
            }
        }
    }

    /**
     * Transfer replicasInfo to list.
     *
     * @param workloadInfo : the workload information from monitor
     * @return list of server workload status.
     */
    private List<StatusItem> decodeClusterWorkloadInfo(String workloadInfo) {
        if (workloadInfo == null || workloadInfo.length() == 0) {
            return null;
        }
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        Map<Integer, Double> cpuCostMap = gson.fromJson(workloadInfo,
                new TypeToken<Map<Integer, Double>>() {
                }.getType());
        Set<Map.Entry<Integer, Double>> serverCosts = cpuCostMap.entrySet();
        List<StatusItem> workloadList = new ArrayList<StatusItem>();
        double localWorkload = 0;
        for (Map.Entry<Integer, Double> entry : serverCosts) {
            workloadList.add(new StatusItem(entry.getKey(), entry.getValue()));
            if (entry.getKey() == serverId) {
                localWorkload = entry.getValue();
            }
        }
        Collections.sort(workloadList);
        double minWorkload = workloadList.get(0).getWorkload();
        if (minWorkload == 0) {
            unbalanceRatio = ConstUtil.UNBALANCE_THRESHOLD;
        } else {
            unbalanceRatio = localWorkload / minWorkload;
        }
        return workloadList;
    }

    /**
     * Create replica for the hot item.
     *
     * @param key : the hot item
     * @return if create the replica succeed.
     */
    private boolean createReplica(String key, int replicaNumber) {
        ConcurrentSkipListSet<Integer> tmp = replicaTable.get(key);
        int oldReplicaNum = (tmp == null) ? 1 : tmp.size();
        if (oldReplicaNum < serversMap.size()) {
            Set<Integer> replicaServers = getReplicaServers(key, replicaNumber);
            Set<Integer> replicas = createReplica(key, replicaServers);
            if (replicas != null) {
                ConcurrentSkipListSet<Integer> set;
                if (!replicaTable.containsKey(key)) {
                    set = new ConcurrentSkipListSet<Integer>();
                    set.add(serverId);
                    replicaTable.put(key, set);
                }
                set = replicaTable.get(key);
                set.addAll(replicas);

                replicaBuffer.put(key, encodeReplicasInfo(set));
                updateReplicaDistribute(key, oldReplicaNum, set.size());

                return true;
            }
        }
        return false;
    }

    private boolean createSingleReplica(String key) {
        return createReplica(key, 1);
    }

    private boolean createMultiReplica(String key) {
        ConcurrentSkipListSet<Integer> tmp = replicaTable.get(key);
        int oldReplicaNum = (tmp == null) ? 1 : tmp.size();
        if (oldReplicaNum < serversMap.size()) {
            int replicaNum = Math.min(oldReplicaNum * 2, serversMap.size() - oldReplicaNum);
            return createReplica(key, replicaNum);
        }
        return false;
    }

    /**
     * Retire replica for the cold item.
     *
     * @param key : the cold item
     * @return if the replica has been retired succeed.
     */
    private boolean retireReplica(String key) {
        if (replicaTable.containsKey(key)) {
            int oldReplicaNum = replicaTable.get(key).size();
            replicaTable.get(key).remove(serverId);
            replicaTable.get(key).remove(replicaTable.get(key).last());
            replicaTable.get(key).add(serverId);

            replicaBuffer.put(key, encodeReplicasInfo(replicaTable.get(key)));
            int replicaNum = replicaTable.get(key).size();
            if (replicaNum == 1) {
                replicaTable.remove(key);
            }
            updateReplicaDistribute(key, oldReplicaNum, replicaNum);
            if (replicaNum != oldReplicaNum) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param key : the hot item
     * @return the location serverId where can a replica be created on for key.
     */
    private Set<Integer> getReplicaServers(String key, int num) {
        Set<Integer> replicaServers = new HashSet<Integer>();
        Set<Integer> currentReplicas = new HashSet<Integer>();
        if (replicaTable.containsKey(key)) {
            currentReplicas = new HashSet<Integer>(replicaTable.get(key));
        } else {
            currentReplicas.add(serverId);
        }

        if (currentReplicas.size() == serversMap.size()) {
            replicaServers.add(DEFAULT_REPLICA_SERVER_ID);
        } else {
            if (replicaMode == ConstUtil.REPLICA_EMBER) {
                for (StatusItem aServer : serverWorkloadList) {
                    int tmp = aServer.getServerId();
                    if (!currentReplicas.contains(tmp)) {
                        replicaServers.add(tmp);
                    }
                    if (replicaServers.size() >= num) {
                        break;
                    }
                }
            } else {
                int tmp, len = serverWorkloadList.size();
                do {
                    int index = new Random().nextInt(len);
                    tmp = serverWorkloadList.get(index).getServerId();
                } while (currentReplicas.contains(tmp));
                replicaServers.add(tmp);
            }
        }

        return replicaServers;
    }

    /**
     * @param key            : the hot item
     * @param replicaServers : the location of replica servers
     * @return the replica servers that has been created succeed.
     */
    private Set<Integer> createReplica(String key, Set<Integer> replicaServers) {
        String value = mClient.get(key);
        if (value == null || value.length() == 0) {
            Log.log.error("[ERROR] no value fo this key: " + key);
            return null;
        }

        Set<Integer> replicas = new HashSet<Integer>();
        for (Integer replicaServerId : replicaServers) {
            if (replicaServerId != DEFAULT_REPLICA_SERVER_ID) {
                DataClient replicaClient;
                if (dataClientMap.containsKey(replicaServerId)) {
                    replicaClient = dataClientMap.get(replicaServerId);
                } else {
                    replicaClient = DataClientFactory.createDataClient(replicaServerId);
                    if (replicaClient != null) {
                        dataClientMap.put(replicaServerId, replicaClient);
                    }
                }
                if (replicaClient != null && replicaClient.set(key, value)) {
                    replicas.add(replicaServerId);
                }
            }
        }

        return replicas;
    }

    private int encodeReplicasInfo(ConcurrentSkipListSet<Integer> replicas) {
        int result = 0;
        for (int id : replicas) {
            result += Math.pow(2, id);
        }
        return result;
    }

    /**
     * Reset the handled hot item.
     *
     * @param key : the hot item
     */
    private void resetHotData(String key) {
        if (hotSpotManager != null) {
            hotSpotManager.resetHotData(key);
        }
    }

    /**
     * Show the replica distribution and push the replica table to clients.
     */
    private void syncReplicaTable() {
        showReplicaDistribution();
        infoAllClient();
    }

    private void showReplicaDistribution() {
        Map<Integer, Integer> map = new HashMap<>();
        Set<Integer> sets = replicaDistribute.keySet();
        for (Integer num : sets) {
            map.put(num, replicaDistribute.get(num).size());
        }
        Log.log.debug("[ReplicaTable] replica distribution " + map.toString());
    }

    /**
     * Push the replica information to clients.
     */
    private void infoAllClient() {
        Map<String, Integer> hotItems = new HashMap<>(replicaBuffer);
        replicaBuffer.clear();
        if (hotItems.size() == 0) {
            return;
        }
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
