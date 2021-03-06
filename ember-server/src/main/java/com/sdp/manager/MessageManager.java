package com.sdp.manager;

/**
 * @author magq
 * MessageManager implement {@link com.sdp.manager.MessageManagerInterface} and handles the messages
 * received by EmberServerHandler, and send the message to other managers according to the message type.
 * <p>
 * There are three different managers to handle different messages: hotSpotManager,
 * replicaManager and consistencyManager.
 * <p>
 * The hotSpotManager{@link com.sdp.manager.hotspotmanager.BaseHotSpotManager} deals with the sampling messages and
 * detects the hot spots by analyzing the request stream. The hot spots found by hotSpotManager
 * will dealt by the replicaManager.
 * <p>
 * The replicaManager{@link com.sdp.manager.ReplicaManager} deals with the replicas management of
 * hot spots, including create replicas for hot spots, retire the redundant replicas and recovery
 * the data.
 * <p>
 * The consistencyManager{@link com.sdp.manager.ConsistencyManager} mainly deals with the write
 * requests and guarantee the consistency among different replicas.
 */

import com.sdp.common.EMSGID;
import com.sdp.log.Log;
import com.sdp.manager.hotspotmanager.BaseHotSpotManager;
import com.sdp.manager.hotspotmanager.HotSpotManagerFactory;
import com.sdp.message.CtsMsg;
import com.sdp.message.CtsMsg.nr_read;
import com.sdp.message.CtsMsg.nr_register;
import com.sdp.netty.NetMsg;
import com.sdp.server.EmberServer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by magq on 16/7/6.
 */
public class MessageManager implements MessageManagerInterface {

    private BaseHotSpotManager hotSpotManager;
    private ReplicaManager replicaManager;
    private ConsistencyManager consistencyManager;

    private EmberServer mServer;
    private ConcurrentHashMap<Integer, Channel> clientChannelMap;

    public MessageManager() {
        replicaManager = new ReplicaManager();
        consistencyManager = new ConsistencyManager();
        init();
    }

    public void init() {
        clientChannelMap = new ConcurrentHashMap<Integer, Channel>();
        replicaManager.setClientChannelMap(clientChannelMap);
    }

    public void initManager(EmberServer server, ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> replicaTable) {
        this.mServer = server;
        replicaManager.initLocalReference(mServer, replicaTable);
        consistencyManager.initLocalReference(mServer, replicaTable);
    }

    /**
     * Call this method to start hot spot detection.
     */
    public void startHotSpotDetection() {
        hotSpotManager = HotSpotManagerFactory.createInstance();
        replicaManager.setHotSpotManager(hotSpotManager);
        hotSpotManager.setOnFindHotSpot(new BaseHotSpotManager.OnFindHotSpot() {
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
        new Thread(hotSpotManager).start();
        new Thread(replicaManager).start();
    }

    public void handleMessage(MessageEvent e) {
        NetMsg msg = (NetMsg) e.getMessage();
        switch (msg.getMsgID()) {
            // connect signal from client
            case nr_connected_mem: {
                handleConnectFromClient(e);
            }
            break;

            // connect signal from server
            case nm_connected: {
                Log.log.info("[Netty] server hear channelConnected from other server: " + e.getChannel());
            }
            break;

            // register signal from client
            case nr_register: {
                handleRegister(msg);
            }
            break;

            // read signal from other manager
            case nr_read: {
                handleRead(e);
            }
            break;

            // write signal from client
            case nr_write: {
                handleWrite(e);
            }
            break;

            default:
                break;
        }
    }

    private void handleConnectFromClient(MessageEvent e) {
        NetMsg msg = (NetMsg) e.getMessage();
        int clientTag = msg.getNodeRoute();
        clientChannelMap.put(clientTag, e.getChannel());
        Log.log.info("[Netty] server hear channelConnected from client: " + e.getChannel());
        CtsMsg.nr_connected_mem_back.Builder builder = CtsMsg.nr_connected_mem_back.newBuilder();
        NetMsg send = NetMsg.newMessage();
        send.setMessageLite(builder);
        send.setMsgID(EMSGID.nr_connected_mem_back);
        e.getChannel().write(send);
    }

    private void handleRegister(NetMsg msg) {
        nr_register msgLite = msg.getMessageLite();
        String key = msgLite.getKey();
        hotSpotManager.handleRegister(key);
    }

    private void handleRead(MessageEvent e) {
        NetMsg msg = (NetMsg) e.getMessage();
        nr_read msgLite = msg.getMessageLite();
        String key = msgLite.getKey();
        int failedId = msg.getNodeRoute();
        if (failedId < 0) {
            int count = -1 * failedId;
            replicaManager.handleRSServers(e.getChannel(), key, count);

        } else {
            replicaManager.handleReadFailed(e.getChannel(), key, failedId);
        }
    }

    private void handleWrite(MessageEvent e) {
        consistencyManager.handleWrite(e);
    }

    static String getOriKey(String key) {
        if (key.contains(":")) {
            return key.substring(key.indexOf(":") + 1);
        }
        return key;
    }
}
