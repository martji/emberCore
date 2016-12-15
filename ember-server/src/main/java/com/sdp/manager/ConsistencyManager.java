package com.sdp.manager;

import com.sdp.common.EMSGID;
import com.sdp.config.ConfigManager;
import com.sdp.message.CtsMsg.nr_write;
import com.sdp.message.CtsMsg.nr_write_res;
import com.sdp.netty.NetMsg;
import com.sdp.replicas.MCThread;
import com.sdp.server.DataClient;
import org.jboss.netty.channel.MessageEvent;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by magq on 16/7/6.
 */
public class ConsistencyManager {

    /**
     * The mClient connect to local data server.
     */
    private DataClient mClient;

    /**
     * Map of the replica location of all hot spots, this will bring some memory overhead.
     */
    private ConcurrentHashMap<String, Vector<Integer>> replicaTable;

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

    public void init() {
        this.pool = Executors.newCachedThreadPool();
    }

    public void initLocalReference(ConcurrentHashMap<String, Vector<Integer>> replicaTable,
                                   ConcurrentHashMap<Integer, DataClient> dataClientMap) {
        this.replicaTable = replicaTable;
        this.dataClientMap = dataClientMap;
        this.mClient = dataClientMap.get(ConfigManager.id);
    }

    public void handleWrite(MessageEvent messageEvent) {
        NetMsg msg = (NetMsg) messageEvent.getMessage();
        nr_write msgLite = msg.getMessageLite();
        int replicasNum = msg.getNodeRoute();
        String key = msgLite.getKey();
        String value = msgLite.getValue();
        String oriKey = MessageManager.getOriKey(key);

        Vector<Future<Boolean>> resultVector = new Vector<Future<Boolean>>();
        int threshold = 0;
        if (replicaTable.containsKey(oriKey)) {
            Vector<Integer> replications = replicaTable.get(oriKey);
            int count = replications.size();
            threshold = getThreshold(count, replicasNum);
            if (threshold > count) {
                threshold = count - 1;
            }
            for (int i = 1; i < count; i++) {
                DataClient mClient = dataClientMap.get(replications.get(i));
                MCThread thread = new MCThread(mClient, key, value);
                Future<Boolean> f = pool.submit(thread);
                resultVector.add(f);
            }
        }

        boolean setState = mClient.set(oriKey, value);
        if (!setState) {
            value = "";
        } else if (threshold > 0) {
            int localCount = 0;
            for (Future<Boolean> f : resultVector) {
                try {
                    if (f.get()) {
                        localCount++;
                    }
                    if (localCount >= threshold) {
                        break;
                    }
                } catch (Exception e2) {
                }
            }
            if (localCount < threshold) {
                value = "";
            }
        }
        NetMsg response = getWriteResponse(key, value);
        messageEvent.getChannel().write(response);
    }

    public Integer getThreshold(int count, int replicasNum) {
        if (replicasNum < 0) {
            return count / replicasNum * -1;
        } else {
            return replicasNum;
        }
    }

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
