package com.sdp.client.ember;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sdp.client.DataClientFactory;
import com.sdp.common.EMSGID;
import com.sdp.log.Log;
import com.sdp.message.CtsMsg.nr_connected_mem;
import com.sdp.message.CtsMsg.nr_read_res;
import com.sdp.message.CtsMsg.nr_replicas_res;
import com.sdp.message.CtsMsg.nr_write_res;
import com.sdp.netty.NetMsg;
import com.sdp.operation.BaseOperation;
import com.sdp.utils.ServerTransUtil;
import org.jboss.netty.channel.*;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * @author martji
 */

public class EmberClientHandler extends SimpleChannelUpstreamHandler {

    private int replicaMode;
    private int clientTag;
    private ConcurrentMap<String, Vector<Integer>> keyReplicaMap;

    private Map<String, BaseOperation<?>> opMap;

    public EmberClientHandler(int replicaMode, int clientTag, ConcurrentMap<String, Vector<Integer>> keyReplicaMap) {
        this.replicaMode = replicaMode;
        this.clientTag = clientTag;
        this.keyReplicaMap = keyReplicaMap;

        opMap = new HashMap<String, BaseOperation<?>>();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        nr_connected_mem.Builder builder = nr_connected_mem.newBuilder();
        NetMsg send = NetMsg.newMessage();
        send.setNodeRoute(clientTag);
        send.setMsgID(EMSGID.nr_connected_mem);
        send.setMessageLite(builder);
        e.getChannel().write(send);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        if (e.getChannel().getLocalAddress() == null) {
            return;
        }
        e.getChannel().close();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        handle(e);
    }

    private void handle(MessageEvent e) throws InterruptedException {
        NetMsg msg = (NetMsg) e.getMessage();
        switch (msg.getMsgID()) {
            case nr_connected_mem_back: {
                Log.log.info("[Netty] connect to server succeed, channel = " + e.getChannel());
            }
            break;
            case nr_replicas_res: {
                nr_replicas_res msgBody = msg.getMessageLite();
                String key = msgBody.getKey();
                String value = msgBody.getValue();
                if (key.length() != 0) {
                    Log.log.debug("[Netty] replication update: " + key + ", " + value);
                    updateKeyReplicaMap(key, value);
                } else {
                    Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
                    Map<String, Integer> replicasMap = gson.fromJson(value,
                            new TypeToken<Map<String, Integer>>() {
                            }.getType());
                    Log.log.debug("[Netty] replica table " + replicasMap);
                    updateKeyReplicaMap(replicasMap);
                }
            }
            break;
            case nr_read_res: {
                nr_read_res msgBody = msg.getMessageLite();
                String key = msgBody.getKey();
                String value = msgBody.getValue();
                handleReadOp(key, value);
            }
            break;
            case nr_write_res: {
                nr_write_res msgBody = msg.getMessageLite();
                String key = msgBody.getKey();
                String value = msgBody.getValue();
                handleWriteOp(key, value);
            }
            break;
            default:
                break;
        }
    }

    private void handleReadOp(String key, String value) {
        if (opMap.containsKey(key)) {
            BaseOperation<String> op = (BaseOperation<String>) opMap.get(key);
            op.getMcallback().gotData(value);
            opMap.remove(key);
        }
    }

    private void handleWriteOp(String key, String value) {
        if (opMap.containsKey(key)) {
            BaseOperation<Boolean> op = (BaseOperation<Boolean>) opMap.get(key);
            if (value != null && value.length() > 0) {
                op.getMcallback().gotData(true);
            } else {
                op.getMcallback().gotData(false);
            }
            opMap.remove(key);
        }
    }

    private void updateKeyReplicaMap(String key, String value) {
        if (value != null && value.length() > 0) {
            int replicaId = Integer.parseInt(value);
            Vector<Integer> result = decodeReplicasInfo(replicaId);
            if (result != null) {
                if (keyReplicaMap.containsKey(key) && result.size() == 1) {
                    keyReplicaMap.remove(key);
                    return;
                }
                keyReplicaMap.put(key, result);
            }
        }
    }

    private void updateKeyReplicaMap(Map<String, Integer> replicasMap) {
        Set<String> keySet = replicasMap.keySet();
        for (String key : keySet) {
            int replicaId = replicasMap.get(key);
            Vector<Integer> result = decodeReplicasInfo(replicaId);
            if (result != null && result.size() > 0) {
                if (replicaMode == DataClientFactory.REPLICA_SPORE) {
                    if(!keyReplicaMap.containsKey(key) && result.size() > 1) {
                        int index = new Random().nextInt(result.size());
                        Vector<Integer> vector = new Vector<Integer>();
                        vector.add(result.get(index));
                        keyReplicaMap.put(key, vector);
                    } else if (keyReplicaMap.containsKey(key) && result.size() == 1) {
                        keyReplicaMap.remove(key);
                    }
                } else {
                    if (keyReplicaMap.containsKey(key) && result.size() == 1) {
                        keyReplicaMap.remove(key);
                        return;
                    }
                    keyReplicaMap.put(key, result);
                }
            }
        }
    }

    void addOpMap(String id, BaseOperation<?> op) {
        opMap.put(id, op);
    }

    private Vector<Integer> decodeReplicasInfo(int value) {
        return ServerTransUtil.decodeServer(value);
    }
}
