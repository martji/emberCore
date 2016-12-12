package com.sdp.client.ember;

import com.sdp.common.EMSGID;
import com.sdp.future.MCallback;
import com.sdp.future.MFuture;
import com.sdp.message.CtsMsg;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;
import com.sdp.netty.NetMsg;
import com.sdp.operation.BaseOperation;
import com.sdp.server.ServerNode;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Guoqing on 2016/4/19.
 */
public class EmberClient {

    private final int TIMEOUT = 2500;
    private final int SAMPLE_RATE = 3;

    private int clientTag;
    private String host;
    private int readPort;
    private int writePort;

    private ClientBootstrap readBootstrap;
    private ClientBootstrap writeBootstrap;

    private Channel readChannel;
    private Channel writeChannel;

    private EmberClientHandler readHandler;
    private EmberClientHandler writeHandler;

    private ConcurrentMap<String, Vector<Integer>> replicaTable;

    public EmberClient(ServerNode node, ConcurrentMap<String, Vector<Integer>> replicaTable) {
        this.replicaTable = replicaTable;
        this.clientTag = (int) System.nanoTime() + new Random().nextInt(100);
        this.host = node.getHost();
        this.readPort = node.getRPort();
        this.writePort = node.getWPort();
        init();
    }

    public void init() {
        try {
            readBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));
            readHandler = new EmberClientHandler(clientTag, replicaTable);
            readBootstrap.setPipelineFactory(new MClientPipelineFactory(readHandler));
            ChannelFuture readFuture = readBootstrap.connect(new InetSocketAddress(host, readPort)).sync();
            while (!readFuture.isDone()) {
            }
            readChannel = readFuture.getChannel();

            writeBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));
            writeHandler = new EmberClientHandler(clientTag, replicaTable);
            writeBootstrap.setPipelineFactory(new MClientPipelineFactory(writeHandler));
            ChannelFuture writeFuture = writeBootstrap.connect(new InetSocketAddress(host, writePort)).sync();
            while (!writeFuture.isDone()) {
            }
            writeChannel = writeFuture.getChannel();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        readChannel.close();
        readBootstrap.releaseExternalResources();
        writeChannel.close();
        writeBootstrap.releaseExternalResources();
    }

    public String get(String key, int failedId) {
        CountDownLatch latch = new CountDownLatch(1);
        BaseOperation<String> op = new BaseOperation<String>(new MCallback<String>(latch));
        MFuture<String> future = new MFuture<String>(latch, op);
        String id = Long.toString(System.currentTimeMillis());
        readHandler.addOpMap(id + ":" + key, op);

        CtsMsg.nr_read.Builder builder = CtsMsg.nr_read.newBuilder();
        builder.setKey(key);
        NetMsg msg = NetMsg.newMessage();
        msg.setNodeRoute(failedId);
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nr_read);

        readChannel.write(msg);

        try {
            return future.get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean set(String key, String value, int replicasNum) {
        CountDownLatch latch = new CountDownLatch(1);
        BaseOperation<Boolean> op = new BaseOperation<Boolean>(new MCallback<Boolean>(latch));
        MFuture<Boolean> future = new MFuture<Boolean>(latch, op);
        String id = Long.toString(System.currentTimeMillis());
        key = id + ":" + key;
        writeHandler.addOpMap(key, op);

        CtsMsg.nr_write.Builder builder = CtsMsg.nr_write.newBuilder();
        builder.setKey(key);
        builder.setValue(value);
        NetMsg msg = NetMsg.newMessage();
        msg.setNodeRoute(replicasNum);
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nr_write);

        writeChannel.write(msg);

        try {
            return future.get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return false;
        }
    }

    public void register(String key) {
        if (!readChannel.isConnected()) {
            return;
        }
        if (isRegister()) {
            CtsMsg.nr_register.Builder builder = CtsMsg.nr_register.newBuilder();
            builder.setKey(key);
            builder.setTime(System.nanoTime());
            NetMsg msg = NetMsg.newMessage();
            msg.setNodeRoute(clientTag);
            msg.setMessageLite(builder);
            msg.setMsgID(EMSGID.nr_register);
            readChannel.write(msg);
        }
    }

    public boolean isRegister() {
        return new Random().nextInt(100) < SAMPLE_RATE;
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
}
