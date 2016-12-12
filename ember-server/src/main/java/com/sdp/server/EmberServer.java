package com.sdp.server;

import com.sdp.client.EmberClient;
import com.sdp.config.ConfigManager;
import com.sdp.log.Log;
import com.sdp.monitor.LocalMonitor;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;
import com.sdp.server.dataclient.DataClientFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * @author martji
 *         Emberserver is the core of this replication middle ware. This server consists of three parts:
 *         connect to the data server(redis, memcached, etc.), handle the connections from clients and
 *         maitain the connection to the global monitor.
 *         <p>
 *         As an universal miidle ware, ember server does not store the data, and all the data is stored
 *         by the data server(redis, memcached, etc.), and ember server hold the dataClientMap{@link DataClient}
 *         to the data servers.
 *         <p>
 *         To enasure ember server can handle the large amounts of connections form clitens, ember server
 *         adopts netty to implement the communication module. Ember server seperates the read requests with
 *         write requests and uses two emberServerHandler{@link EmberServerHandler} to deal with the requests
 *         to achieve higher throughput.
 *         <p>
 *         The monitorClient{@link EmberClient} keep connection with the global monitor, and can get the
 *         workload informations by this connection.
 */

public class EmberServer {

    private ConcurrentHashMap<Integer, DataClient> dataClientMap;

    private int readPort;
    private int writePort;

    private EmberServerHandler wServerHandler;
    private EmberServerHandler rServerHandler;
    private EmberClient monitorClient;

    private static final int SLEEP_TIME = 30 * 1000;

    public EmberServer() {
        initDataClientMap();
    }

    /**
     * Init the ember server, and register to the monitor.
     *
     * @param id
     * @param serversMap
     * @param wServerHandler
     * @param rServerHandler
     */
    public void init(int id, Map<Integer, EmberServerNode> serversMap,
                     EmberServerHandler wServerHandler, EmberServerHandler rServerHandler) {
        this.wServerHandler = wServerHandler;
        this.rServerHandler = rServerHandler;

        EmberServerNode serverNode = serversMap.get(id);
        readPort = serverNode.getReadPort();
        writePort = serverNode.getWritePort();
        initEmberServer();

        registerMonitor();
    }

    public void init(EmberServerHandler wServerHandler, EmberServerHandler rServerHandler) {
        init(ConfigManager.id, ConfigManager.serversMap, wServerHandler, rServerHandler);
    }

    /**
     * Connect to the data servers.
     */
    public void initDataClientMap() {
        dataClientMap = new ConcurrentHashMap<Integer, DataClient>();
        Map<Integer, EmberServerNode> serversMap = ConfigManager.serversMap;
        Iterator<Map.Entry<Integer, EmberServerNode>> iterator = serversMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, EmberServerNode> map = iterator.next();
            int id = map.getKey();
            DataClient dataClient = DataClientFactory.createDataClient(id);
            dataClientMap.put(id, dataClient);
        }
        Log.log.info("[Ember server] finish init data clients");
    }

    private void registerMonitor() {
        registerMonitor(ConfigManager.id, (String) ConfigManager.propertiesMap.get(ConfigManager.MONITOR_ADDRESS),
                ConfigManager.serversMap.get(ConfigManager.id).getDataPort());
    }

    /**
     * @param id             : the id of the server instance
     * @param monitorAddress : the address of the monitor node
     * @param serverPort     : the serverPort
     */
    private void registerMonitor(int id, String monitorAddress, int serverPort) {
        LocalMonitor.getInstance().setPort(serverPort);
        Log.log.info("[Monitor] " + monitorAddress);
        String[] arr = monitorAddress.split(":");
        final String host = arr[0];
        final int port = Integer.parseInt(arr[1]);

        monitorClient = new EmberClient(id, host, port);
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(SLEEP_TIME);
                        if (monitorClient.getMChannel() == null || !monitorClient.getMChannel().isConnected()) {
                            monitorClient.connect();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public void initEmberServer() {
        ServerBootstrap wBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));
        wBootstrap.setPipelineFactory(new MServerPipelineFactory(wServerHandler));
        wBootstrap.setOption("child.tcpNoDelay", true);
        wBootstrap.setOption("child.keepAlive", true);
        wBootstrap.setOption("reuseAddress", true);
        wBootstrap.bind(new InetSocketAddress(writePort));

        ServerBootstrap rBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));
        rBootstrap.setPipelineFactory(new MServerPipelineFactory(rServerHandler));
        rBootstrap.setOption("child.tcpNoDelay", true);
        rBootstrap.setOption("child.keepAlive", true);
        rBootstrap.setOption("reuseAddress", true);
        rBootstrap.bind(new InetSocketAddress(readPort));

        Log.log.info("[Netty] ember server start");
    }

    public String getAReplica() {
        if (monitorClient == null || monitorClient.getMChannel() == null) {
            Log.log.warn("[Netty] connect to monitor failed");
            if (monitorClient != null) {
                monitorClient.connect();
            }
            return null;
        } else {
            return monitorClient.asyncGetAReplica();
        }
    }

    private class MServerPipelineFactory implements ChannelPipelineFactory {
        EmberServerHandler mServerHandler;

        public MServerPipelineFactory(EmberServerHandler mServerHandler) {
            this.mServerHandler = mServerHandler;
        }

        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("decoder", new MDecoder());
            pipeline.addLast("encoder", new MEncoder());
            pipeline.addLast("handler", mServerHandler);
            return pipeline;
        }
    }

    public ConcurrentHashMap<Integer, DataClient> getDataClientMap() {
        return dataClientMap;
    }
}
