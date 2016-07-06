package com.sdp.manager;

import com.sdp.common.EMSGID;
import com.sdp.messageBody.CtsMsg.nr_write;
import com.sdp.messageBody.CtsMsg.nr_write_res;
import com.sdp.netty.NetMsg;
import com.sdp.replicas.MCThread;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by magq on 16/7/6.
 */
public class ConsistencyManager {

    private final int EXPIRE_TIME = 60*60*24*10;

    /**
     * The memcachedClient connect to local memcached server.
     */
    private MemcachedClient memcachedClient;

    /**
     * Map of the replica location of all hot spots, this will bring some memory overhead.
     */
    private ConcurrentHashMap<String, Vector<Integer>> replicasIdMap;

    /**
     * The map of connection to other memcached servers.
     */
    private ConcurrentHashMap<Integer, MemcachedClient> spyClientMap;

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

    public void initLocalReference(MemcachedClient client,
                                   ConcurrentHashMap<String, Vector<Integer>> replicasIdMap,
                                   ConcurrentHashMap<Integer, MemcachedClient> spyClientMap) {
        this.memcachedClient = client;
        this.replicasIdMap = replicasIdMap;
        this.spyClientMap = spyClientMap;
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
        if (replicasIdMap.containsKey(oriKey)) {
            Vector<Integer> replications = replicasIdMap.get(oriKey);
            int count = replications.size();
            threshold = getThreshold(count, replicasNum);
            if (threshold > count) {
                threshold = count - 1;
            }
            for (int i = 1; i < count; i++) {
                MemcachedClient mClient = spyClientMap.get(replications.get(i));
                MCThread thread = new MCThread(mClient, key, value);
                Future<Boolean> f = pool.submit(thread);
                resultVector.add(f);
            }
        }

        OperationFuture<Boolean> res = memcachedClient.set(oriKey, EXPIRE_TIME, value);
        boolean setState = getSetState(res);
        if (!setState) {
            value = "";
        } else if (threshold > 0){
            int localCount = 0;
            for (Future<Boolean> f : resultVector) {
                try {
                    if (f.get()) {
                        localCount ++;
                    }
                    if (localCount >= threshold) {
                        break;
                    }
                } catch (Exception e2) {}
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
            return count/replicasNum*-1;
        } else {
            return replicasNum;
        }
    }

    public boolean getSetState(OperationFuture<Boolean> res) {
        try {
            if (res.get()) {
                return true;
            }
        } catch (Exception e) {}
        return false;
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
