package com.sdp.manager;

import com.sdp.common.EMSGID;
import com.sdp.config.ConfigManager;
import com.sdp.message.CtsMsg.nr_write;
import com.sdp.message.CtsMsg.nr_write_res;
import com.sdp.netty.NetMsg;
import com.sdp.replicas.MCSetThread;
import com.sdp.server.DataClient;
import com.sdp.server.EmberServer;
import org.jboss.netty.channel.MessageEvent;

import java.util.Set;
import java.util.Vector;
import java.util.concurrent.*;

/**
 * Created by magq on 16/7/6.
 */
public class ConsistencyManager {

    private final int TIMEOUT = 2500;

    /**
     * The mClient connect to local data server.
     */
    private DataClient mClient;
    private int serverId;

    /**
     * Map of the replica location of all hot spots, this will bring some memory overhead.
     */
    private ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> replicaTable;

    /**
     * The map of connection to other data servers.
     */
    private ConcurrentHashMap<Integer, DataClient> dataClientMap;

    /**
     * A thread pool to submit write operation.
     */
    private ExecutorService pool;

    public ConsistencyManager() {
        init();
    }

    private void init() {
        this.pool = Executors.newCachedThreadPool();
    }

    void initLocalReference(EmberServer server, ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> replicaTable) {
        this.replicaTable = replicaTable;
        this.dataClientMap = server.getDataClientMap();
        this.serverId = ConfigManager.id;
        this.mClient = dataClientMap.get(serverId);
    }

    /**
     * Handle the write request.
     *
     * @param messageEvent : the write message
     */
    void handleWrite(MessageEvent messageEvent) {
        NetMsg msg = (NetMsg) messageEvent.getMessage();
        nr_write msgLite = msg.getMessageLite();
        int replicasNum = msg.getNodeRoute();
        String key = msgLite.getKey();
        String value = msgLite.getValue();
        String oriKey = MessageManager.getOriKey(key);

        boolean result = true;
        if (!replicaTable.containsKey(oriKey) || replicasNum == 0) {
            result = mClient.set(oriKey, value);
        } else {
            Set<Integer> replicas = new ConcurrentSkipListSet<>(replicaTable.get(oriKey));
            replicas.remove(serverId);
            replicasNum = Math.min(replicasNum, replicas.size());
            CountDownLatch latch = new CountDownLatch(replicasNum);
            Vector<Boolean> values = new Vector<Boolean>();
            for (Integer replicaServerId : replicas) {
                MCSetThread thread = new MCSetThread(dataClientMap.get(replicaServerId), oriKey, value, latch, values);
                pool.submit(thread);
            }
            try {
                result = mClient.set(oriKey, value);
                latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
                for (int i = 0; i < values.size() && result; i++) {
                    result = values.get(i);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        value = result ? value : "";
        NetMsg response = getWriteResponse(key, value);
        messageEvent.getChannel().write(response);
    }

    /**
     * @deprecated
     */
    public Integer getThreshold(int count, int replicasNum) {
        if (replicasNum < 0) {
            return count / replicasNum * -1;
        } else {
            return replicasNum;
        }
    }

    /**
     * Package the write response.
     */
    private NetMsg getWriteResponse(String key, String value) {
        nr_write_res.Builder builder = nr_write_res.newBuilder();
        builder.setKey(key);
        builder.setValue(value);
        NetMsg send = NetMsg.newMessage();
        send.setMessageLite(builder);
        send.setMsgID(EMSGID.nr_write_res);
        return send;
    }
}
