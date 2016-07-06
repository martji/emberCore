package com.sdp.manager;

/**
 * This class only handle the message received by MServerHandler, and send the
 * message to other managers according to the message type.
 */

import com.sdp.common.EMSGID;
import com.sdp.hotspot.BaseHotspotDetector;
import com.sdp.messageBody.CtsMsg;
import com.sdp.messageBody.CtsMsg.nr_read;
import com.sdp.messageBody.CtsMsg.nr_register;
import com.sdp.netty.NetMsg;
import com.sdp.server.MServer;
import net.spy.memcached.MemcachedClient;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.internal.ConcurrentHashMap;

/**
 * Created by magq on 16/7/6.
 */
public class MessageManager {

    private HotSpotManager hotSpotManager;

    private ReplicaManager replicaManager;

    private ConsistencyManager consistencyManager;

    private ConcurrentHashMap<Integer, Channel> clientChannelMap;

    public MessageManager() {
        hotSpotManager = new HotSpotManager();
        replicaManager = new ReplicaManager();
        consistencyManager = new ConsistencyManager();

        clientChannelMap = new ConcurrentHashMap<Integer, Channel>();

        hotSpotManager.setOnFindHotSpot(new BaseHotspotDetector.OnFindHotSpot() {
            public void dealHotSpot() {
                replicaManager.dealHotData();
            }

            public void dealColdSpot() {
                replicaManager.dealColdData();
            }

            public void dealHotSpot(String key) {
                replicaManager.dealHotData(key);
            }
        });

        replicaManager.setClientChannelMap(clientChannelMap);
    }

    public void initReplicaManager(MServer server, MemcachedClient client, int serverId) {
        replicaManager.initLocalReference(server, client, serverId);
    }

    public void handleMessage(MessageEvent messageEvent) {
        NetMsg msg = (NetMsg) messageEvent.getMessage();
        switch (msg.getMsgID()) {
            // connect signal from client
            case nr_connected_mem: {
                handleConnectFromClient(messageEvent);
            }
            break;

            // connect signal from server
            case nm_connected: {
                System.out.println("[Netty] server hear channelConnected from other server: " +
                        messageEvent.getChannel());
            }
            break;

            // register signal from client
            case nr_register: {
                handleRegister(msg);
            }
            break;

            // read signal from other manager
            case nr_read: {
                handleRead(messageEvent);
            }
            break;

            // write signal from client
            case nr_write: {
                handleWrite(messageEvent);
            }
            break;

            default:
                break;
        }
    }

    public void handleConnectFromClient(MessageEvent messageEvent) {
        NetMsg msg = (NetMsg) messageEvent.getMessage();
        int clientId = msg.getNodeRoute();
        clientChannelMap.put(clientId, messageEvent.getChannel());
        System.out.println("[Netty] server hear channelConnected from client: " + messageEvent.getChannel());
        CtsMsg.nr_connected_mem_back.Builder builder = CtsMsg.nr_connected_mem_back.newBuilder();
        NetMsg send = NetMsg.newMessage();
        send.setMessageLite(builder);
        send.setMsgID(EMSGID.nr_connected_mem_back);
        messageEvent.getChannel().write(send);
    }

    public void handleRegister(NetMsg msg) {
        nr_register msgLite = msg.getMessageLite();
        String key = msgLite.getKey();
        hotSpotManager.handleRegister(key);
    }

    public void handleRead(MessageEvent messageEvent) {
        NetMsg msg = (NetMsg) messageEvent.getMessage();
        nr_read msgLite = msg.getMessageLite();
        String key = msgLite.getKey();
        int failedId = msg.getNodeRoute();
        replicaManager.handleReadFailed(messageEvent.getChannel(), key, failedId);
    }

    public void handleWrite(MessageEvent messageEvent) {
        consistencyManager.handleWrite(messageEvent);
    }

}
