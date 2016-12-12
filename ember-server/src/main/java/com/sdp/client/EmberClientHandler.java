package com.sdp.client;

import com.sdp.common.EMSGID;
import com.sdp.log.Log;
import com.sdp.message.CtsMsg.nr_apply_replica_res;
import com.sdp.message.CtsMsg.nr_cpuStats_res;
import com.sdp.message.StsMsg.nm_connected;
import com.sdp.message.StsMsg.nm_read;
import com.sdp.message.StsMsg.nm_read_recovery;
import com.sdp.monitor.LocalMonitor;
import com.sdp.netty.NetMsg;
import com.sdp.operation.BaseOperation;
import org.jboss.netty.channel.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author martji
 */

public class EmberClientHandler extends SimpleChannelUpstreamHandler {

    public int id;
    Map<String, BaseOperation<?>> opMap;

    public EmberClientHandler(int id) {
        this.id = id;
        opMap = new HashMap<String, BaseOperation<?>>();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        nm_connected.Builder builder = nm_connected.newBuilder();
        builder.setNum(this.id);
        NetMsg sendMsg = NetMsg.newMessage();
        sendMsg.setNodeRoute(id);
        sendMsg.setMsgID(EMSGID.nm_connected);
        sendMsg.setMessageLite(builder);
        e.getChannel().write(sendMsg);
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
            case nm_connected_mem_back: {
                Log.log.info("[Netty] connect to monitor succeed " + e.getChannel());
            }
            break;
            case nr_stats: {
                Double cpuCost = LocalMonitor.getInstance().getCpuCost();

                nr_cpuStats_res.Builder builder = nr_cpuStats_res.newBuilder();
                builder.setValue(cpuCost.toString());
                NetMsg send = NetMsg.newMessage();
                send.setMessageLite(builder);
                send.setNodeRoute(id);
                send.setMsgID(EMSGID.nr_stats_res);

                e.getChannel().write(send);
            }
            break;
            case nr_apply_replica_res: {
                nr_apply_replica_res msgLite = msg.getMessageLite();
                String key = msgLite.getKey();
                String value = msgLite.getValue();
                handleStatsOp(key, value);
            }
            break;
            case nm_read: {
                nm_read msgLite = msg.getMessageLite();
                String key = msgLite.getKey();
                String value = msgLite.getValue();
                handleNmReadOp(key, value);
            }
            break;
            case nm_read_recovery: {
                nm_read_recovery msgLite = msg.getMessageLite();
                String id = msgLite.getKey();
                String value = msgLite.getValue();
                handleNmRecOp(id, value);
            }
            break;
            default:
                break;
        }
    }

    @SuppressWarnings("unchecked")
    private void handleStatsOp(String key, String value) {
        if (opMap.containsKey(key)) {
            BaseOperation<String> op = (BaseOperation<String>) opMap.get(key);
            op.getMcallback().gotdata(value);
            opMap.remove(key);
        }
    }

    private void handleNmRecOp(String key, String value) {
        if (opMap.containsKey(key)) {
            @SuppressWarnings("unchecked")
            BaseOperation<Boolean> op = (BaseOperation<Boolean>) opMap.get(key);
            if (value != null && value.length() > 0) {
                op.getMcallback().gotdata(true);
            } else {
                op.getMcallback().gotdata(false);
            }
            opMap.remove(key);
        }
    }

    private void handleNmReadOp(String key, String value) {
        if (opMap.containsKey(key)) {
            @SuppressWarnings("unchecked")
            BaseOperation<String> op = (BaseOperation<String>) opMap.get(key);
            op.getMcallback().gotdata(value);
            opMap.remove(key);
        }
    }

    public void addOpMap(String id, BaseOperation<?> op) {
        opMap.put(id, op);
    }
}
