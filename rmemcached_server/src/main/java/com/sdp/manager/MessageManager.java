package com.sdp.manager;

/**
 * This class only handle the message received by MServerHandler, and send the
 * message to other managers according to the message type.
 */

import com.sdp.common.EMSGID;
import com.sdp.config.GlobalConfigMgr;
import com.sdp.example.Log;
import com.sdp.hotspot.BaseHotspotDetector;
import com.sdp.messageBody.CtsMsg;
import com.sdp.messageBody.CtsMsg.nr_read;
import com.sdp.messageBody.CtsMsg.nr_register;
import com.sdp.netty.NetMsg;
import com.sdp.server.MServer;
import com.sdp.server.ServerNode;
import net.spy.memcached.MemcachedClient;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

/**
 * Created by magq on 16/7/6.
 */
public class MessageManager {

    private FrequentContrastManager hotSpotManager;

    private ReplicaManager replicaManager;

    private ConsistencyManager consistencyManager;

    private MServer mServer;
    private ConcurrentHashMap<Integer, Channel> clientChannelMap;

    public void setMServer(MServer server) {
        this.mServer = server;
        replicaManager.setServer(mServer);
    }

    /**
     * The map of connection to other memcached servers.
     */
    private ConcurrentHashMap<Integer, MemcachedClient> spyClientMap;

    public MessageManager() {
        hotSpotManager = new FrequentContrastManager();
        replicaManager = new ReplicaManager();
        consistencyManager = new ConsistencyManager();

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

        init();
    }

    public void init() {
        initSpyClientMap();

        clientChannelMap = new ConcurrentHashMap<Integer, Channel>();
        replicaManager.setClientChannelMap(clientChannelMap);
    }

    public void initManager(MServer server, MemcachedClient client) {
        initManager(server, client, new ConcurrentHashMap<String, Vector<Integer>>());
    }

    public void initManager(MServer server, MemcachedClient client,
                            ConcurrentHashMap<String, Vector<Integer>> replicasIdMap) {
        this.mServer = server;
        replicaManager.initLocalReference(mServer, client, replicasIdMap, spyClientMap);
        consistencyManager.initLocalReference(client, replicasIdMap, spyClientMap);
    }

    public void initSpyClientMap() {
        spyClientMap = new ConcurrentHashMap<Integer, MemcachedClient>();
        new Thread(new Runnable() {
            public void run() {
                int serverId = GlobalConfigMgr.id;
                Map<Integer, ServerNode> serversMap = GlobalConfigMgr.serversMap;

                Iterator<Map.Entry<Integer, ServerNode>> iterator = serversMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Integer, ServerNode> map = iterator.next();
                    int id = map.getKey();
                    if (id != serverId) {
                        MemcachedClient spyClient = buildAMClient(id);
                        spyClientMap.put(id, spyClient);
                    }
                }
            }
        }).start();
    }

    /**
     * Call this method to start hot spot detection.
     */
    public void startHotSpotDetection() {
        new Thread(replicaManager).start();
        new Thread(hotSpotManager).start();
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
                Log.log.info("[Netty] server hear channelConnected from other server: " +
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
        Log.log.info("[Netty] server hear channelConnected from client: " + messageEvent.getChannel());
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

    /**
     *
     * @param replicaId
     * @return the spyClient to server.
     */
    public static MemcachedClient buildAMClient(int replicaId){
        try {
            MemcachedClient replicaClient;
            Map<Integer, ServerNode> serversMap = GlobalConfigMgr.serversMap;
            ServerNode serverNode = serversMap.get(replicaId);
            String host = serverNode.getHost();
            int port = serverNode.getMemcached();
            replicaClient = new MemcachedClient(new InetSocketAddress(host, port));
            return replicaClient;
        } catch (Exception e) {}
        return null;
    }

    public static String getOriKey(String key) {
        if (key.contains(":")) {
            return key.substring(key.indexOf(":") + 1);
        }
        return key;
    }
}
