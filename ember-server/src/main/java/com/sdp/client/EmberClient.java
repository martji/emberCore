package com.sdp.client;

import com.sdp.common.EMSGID;
import com.sdp.future.MCallback;
import com.sdp.future.MFuture;
import com.sdp.log.Log;
import com.sdp.message.CtsMsg.nr_apply_replica;
import com.sdp.message.StsMsg.nm_read;
import com.sdp.message.StsMsg.nm_read_recovery;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;
import com.sdp.netty.NetMsg;
import com.sdp.operation.BaseOperation;
import com.sdp.server.EmberServerNode;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author martji
 *         <p>
 *         Common client of netty.
 */

public class EmberClient {

    private static final long TIMEOUT = 2500;

    private int serverId;
    private String host;
    private int port;

    private ClientBootstrap bootstrap;
    private Channel mChannel;
    private EmberClientHandler mClientHandler;

    public EmberClient(int id, String host, int port) {
        this.serverId = id;
        this.host = host;
        this.port = port;
        init();
    }

    public EmberClient(EmberServerNode serverNode) {
        int serverId = serverNode.getId();
        String host = serverNode.getHost();
        int port = serverNode.getReadPort();

        this.serverId = serverId;
        this.host = host;
        this.port = port;
        init();
    }

    public EmberClient(int serverId, EmberServerNode serverNode) {
        String host = serverNode.getHost();
        int port = serverNode.getReadPort();

        this.serverId = serverId;
        this.host = host;
        this.port = port;
        init();
    }

    public void init() {
        try {
            bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));
            mClientHandler = new EmberClientHandler(serverId);
            bootstrap.setPipelineFactory(new MClientPipelineFactory(mClientHandler));

            connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void shutdown() {
        mChannel.close();
        bootstrap.releaseExternalResources();
    }

    public void connect() {
        try {
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port)).sync();
            while (!future.isDone()) {
            }
            mChannel = future.getChannel();
            Log.log.info("[Netty] monitor channel " + mChannel);
        } catch (Exception e) {
            Log.log.error("[Netty] can not connect to monitor");
        }
    }

    public String asyncGetClusterWorkload() {
        CountDownLatch latch = new CountDownLatch(1);
        BaseOperation<String> op = new BaseOperation<String>(new MCallback<String>(latch));
        MFuture<String> future = new MFuture<String>(latch, op);
        String id = Long.toString(System.currentTimeMillis());
        mClientHandler.addOpMap(id, op);

        nr_apply_replica.Builder builder = nr_apply_replica.newBuilder();
        builder.setKey(id);
        NetMsg msg = NetMsg.newMessage();
        msg.setNodeRoute(serverId);
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nr_apply_replica);
        if (!mChannel.isConnected()) {
            Log.log.warn("[Netty] lose connection with monitor");
            connect();
            return null;
        }
        mChannel.write(msg);

        try {
            return future.get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @deprecated
     * @param key
     * @return
     */
    public String readFromReplica(String key) {
        CountDownLatch latch = new CountDownLatch(1);
        BaseOperation<String> op = new BaseOperation<String>(new MCallback<String>(latch));
        MFuture<String> future = new MFuture<String>(latch, op);
        String id = Long.toString(System.currentTimeMillis());
        key = id + ":" + key;
        mClientHandler.addOpMap(key, op);

        nm_read.Builder builder = nm_read.newBuilder();
        builder.setKey(key);
        NetMsg msg = NetMsg.newMessage();
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nm_read);
        mChannel.write(msg);

        try {
            return future.get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * @deprecated
     * @param key
     * @param value
     * @return
     */
    public boolean recoveryAReplica(String key, String value) {
        CountDownLatch latch = new CountDownLatch(1);
        BaseOperation<Boolean> op = new BaseOperation<Boolean>(new MCallback<Boolean>(latch));
        MFuture<Boolean> future = new MFuture<Boolean>(latch, op);
        String id = Long.toString(System.currentTimeMillis());
        key = id + ":" + key;
        mClientHandler.addOpMap(key, op);

        nm_read_recovery.Builder builder = nm_read_recovery.newBuilder();
        builder.setKey(key);
        builder.setValue(value);
        NetMsg msg = NetMsg.newMessage();
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nm_read_recovery);
        mChannel.write(msg);

        try {
            return future.get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    private class MClientPipelineFactory implements ChannelPipelineFactory {
        private EmberClientHandler mClientHandler;

        public MClientPipelineFactory(EmberClientHandler mClientHandler) {
            this.mClientHandler = mClientHandler;
        }

        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("decoder", new MDecoder());
            pipeline.addLast("encoder", new MEncoder());
            pipeline.addLast("handler", mClientHandler);
            return pipeline;
        }
    }

    public Channel getMChannel() {
        return mChannel;
    }
}
