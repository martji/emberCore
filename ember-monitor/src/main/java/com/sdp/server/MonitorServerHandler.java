package com.sdp.server;

import com.sdp.common.EMSGID;
import com.sdp.log.Log;
import com.sdp.messagebody.CtsMsg.nr_apply_replica;
import com.sdp.messagebody.CtsMsg.nr_apply_replica_res;
import com.sdp.messagebody.CtsMsg.nr_cpuStats_res;
import com.sdp.messagebody.StsMsg.nm_connected_mem_back;
import com.sdp.manager.MonitorManager;
import com.sdp.netty.NetMsg;
import org.jboss.netty.channel.*;

/**
 * @author martji
 */

public class MonitorServerHandler extends SimpleChannelUpstreamHandler {

    MonitorManager monitor;

    public MonitorServerHandler() {
        monitor = new MonitorManager();
        monitor.start();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        handleMessage(e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        Channel channel = e.getChannel();
        channel.close();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
    }

    private void handleMessage(MessageEvent e) {
        NetMsg msg = (NetMsg) e.getMessage();
        switch (msg.getMsgID()) {
            case nm_connected: {
                int clientNode = msg.getNodeRoute();
                Log.log.info("[Netty] register: instanceId = " + clientNode + " channel = " + e.getChannel());
                monitor.addServer(clientNode, e.getChannel());

                nm_connected_mem_back.Builder builder = nm_connected_mem_back.newBuilder();
                NetMsg send = NetMsg.newMessage();
                send.setMessageLite(builder);
                send.setMsgID(EMSGID.nm_connected_mem_back);

                e.getChannel().write(send);
            }
            break;
            case nr_stats_res: {
                int clientNode = msg.getNodeRoute();
                nr_cpuStats_res msgLite = msg.getMessageLite();
                String cpuCost = msgLite.getValue();
                monitor.handle(clientNode, cpuCost);
            }
            break;
            case nr_apply_replica: {
                String replicas = monitor.chooseReplica();
                nr_apply_replica msgLite = msg.getMessageLite();
                String id = msgLite.getKey();

                nr_apply_replica_res.Builder builder = nr_apply_replica_res.newBuilder();
                builder.setKey(id);
                builder.setValue(replicas);
                NetMsg send = NetMsg.newMessage();
                send.setMessageLite(builder);
                send.setMsgID(EMSGID.nr_apply_replica_res);
                e.getChannel().write(send);
            }
            break;
            default:
                break;
        }
    }
}

