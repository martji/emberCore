package com.sdp.client;

import com.sdp.common.EMSGID;
import com.sdp.future.MCallback;
import com.sdp.future.MFuture;
import com.sdp.messageBody.CtsMsg;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;
import com.sdp.netty.NetMsg;
import com.sdp.operation.BaseOperation;
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

    private final static int TIMEOUT = 2500;
    private final static int SAMPRATE = 3;

    private ClientBootstrap rbootstrap;
    private ClientBootstrap wbootstrap;

    private Channel rmChannel = null;
    private Channel wmChannel = null;

    private RMemcachedClientImplHandler rmClientHandler;
    private RMemcachedClientImplHandler wmClientHandler;

    private StringBuffer message = new StringBuffer();
    private ConcurrentMap<String, Vector<Integer>> keyReplicaMap;

    public EmberClient(ConcurrentMap<String, Vector<Integer>> replicaMap,
                       int clientNode, String host, int rport, int wport) {
        this.keyReplicaMap = replicaMap;
        try {
            rbootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));
            rmClientHandler = new RMemcachedClientImplHandler(clientNode, message, keyReplicaMap);
            rbootstrap.setPipelineFactory(new MClientPipelineFactory(rmClientHandler));
            ChannelFuture rfuture = rbootstrap.connect(new InetSocketAddress(host, rport)).sync();
            while (!rfuture.isDone()) {}
            rmChannel = rfuture.getChannel();

            wbootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));
            wmClientHandler = new RMemcachedClientImplHandler(clientNode, message, keyReplicaMap);
            wbootstrap.setPipelineFactory(new MClientPipelineFactory(wmClientHandler));
            ChannelFuture wfuture = wbootstrap.connect(new InetSocketAddress(host, wport)).sync();
            while (!wfuture.isDone()) {}
            wmChannel = wfuture.getChannel();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean hasReplica(String key) {
        return keyReplicaMap.containsKey(key);
    }

    public boolean set(String key, String value, int replicasNum) {
        CountDownLatch latch = new CountDownLatch(1);
        BaseOperation<Boolean> op = new BaseOperation<Boolean>(new MCallback<Boolean>(latch));
        MFuture<Boolean> future = new MFuture<Boolean>(latch, op);
        String id = Long.toString(System.currentTimeMillis());
        key = id + ":" + key;
        wmClientHandler.addOpMap(key, op);

        CtsMsg.nr_write.Builder builder = CtsMsg.nr_write.newBuilder();
        builder.setKey(key);
        builder.setValue(value);
        NetMsg msg = NetMsg.newMessage();
        msg.setNodeRoute(replicasNum);
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nr_write);

        wmChannel.write(msg);

        try {
            return future.get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return false;
        }
    }

    public String get(final String key, int failedId) {
        CountDownLatch latch = new CountDownLatch(1);
        BaseOperation<String> op = new BaseOperation<String>(new MCallback<String>(latch));
        MFuture<String> future = new MFuture<String>(latch, op);
        String id = Long.toString(System.currentTimeMillis());
        rmClientHandler.addOpMap(id + ":" + key, op);

        CtsMsg.nr_read.Builder builder = CtsMsg.nr_read.newBuilder();
        builder.setKey(key);
        NetMsg msg = NetMsg.newMessage();
        msg.setNodeRoute(failedId);
        msg.setMessageLite(builder);
        msg.setMsgID(EMSGID.nr_read);

        rmChannel.write(msg);

        try {
            return future.get(TIMEOUT , TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void register(String key, int clientId) {
        if (need2Register()) {
            CtsMsg.nr_register.Builder builder = CtsMsg.nr_register.newBuilder();
            builder.setKey(key);
            builder.setTime(System.nanoTime());
            NetMsg msg = NetMsg.newMessage();
            msg.setNodeRoute(clientId);
            msg.setMessageLite(builder);
            msg.setMsgID(EMSGID.nr_register);
            rmChannel.write(msg);
        }
    }

    public boolean need2Register() {
        Random random = new Random();
        return random.nextInt(100) < SAMPRATE;
    }

    public void shutdown() {
        rmChannel.close();
        rbootstrap.releaseExternalResources();
        wmChannel.close();
        wbootstrap.releaseExternalResources();
    }

    private class MClientPipelineFactory implements ChannelPipelineFactory {
        private RMemcachedClientImplHandler mClientHandler;

        public MClientPipelineFactory(RMemcachedClientImplHandler mClientHandler) {
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
