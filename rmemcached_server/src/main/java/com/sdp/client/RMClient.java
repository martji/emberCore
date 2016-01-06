package com.sdp.client;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.sdp.common.EMSGID;
import com.sdp.future.MCallback;
import com.sdp.future.MFuture;
import com.sdp.messageBody.CtsMsg.nr_apply_replica;
import com.sdp.messageBody.StsMsg.nm_read;
import com.sdp.messageBody.StsMsg.nm_read_recovery;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;
import com.sdp.netty.NetMsg;
import com.sdp.operation.BaseOperation;
import com.sdp.server.ServerNode;

/**
 * 
 * @author martji
 * 
 */

public class RMClient{

	int serverId;
	String host;
	int port;
	ClientBootstrap bootstrap;
	Channel mChannel = null;
	RMClientHandler mClientHandler;
	private static long timeout = 2500;

	public RMClient(int id, String host, int port) {
		this.serverId = id;
		this.host = host;
		this.port = port;
		init(id, host, port);
	}
	
	public RMClient(ServerNode serverNode) {
		int serverId = serverNode.getId();
		String host = serverNode.getHost();
		int port = serverNode.getRPort();
		
		this.serverId = serverId;
		this.host = host;
		this.port = port;
		init(serverId, host, port);
	}
	
	public RMClient(int serverId, ServerNode serverNode) {
		String host = serverNode.getHost();
		int port = serverNode.getRPort();
		
		this.serverId = serverId;
		this.host = host;
		this.port = port;
		init(serverId, host, port);
	}

	public void init() {
		init(0, "127.0.0.1", 8080);
	}

	public void shutdown() {
		mChannel.close();
	}
	
	public void init(int id, String host, int port) {
		try {
			bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
					Executors.newCachedThreadPool(),
					Executors.newCachedThreadPool()));

			mClientHandler = new RMClientHandler(id);
			bootstrap.setPipelineFactory(new MClientPipelineFactory(mClientHandler));

			try {
				ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port)).sync();
				while (!future.isDone()) {}
				mChannel = future.getChannel();
			} catch (ChannelException e) {}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public void reconnect() {
		try {
			ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port)).sync();
			while (!future.isDone()) {}
			mChannel = future.getChannel();
		} catch (Exception e) {}
	}
	
	public void connect(String host, int port) {
		try {
			ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port)).sync();
			while (!future.isDone()) {}
			mChannel = future.getChannel();
		} catch (Exception e) {}
	}
	
	public void connect(ServerNode serverNode) {
		String host = serverNode.getHost();
		int port = serverNode.getRPort();
		connect(host, port);
	}
	
	public String asynGetAReplica() {
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
		mChannel.write(msg);
		
		try {
			return future.get(timeout, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
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
			return future.get(timeout , TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
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
			return future.get(timeout , TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	

	private class MClientPipelineFactory implements ChannelPipelineFactory {
		private RMClientHandler mClientHandler;

		public MClientPipelineFactory(RMClientHandler mClientHandler) {
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
	
	public Channel getmChannel() {
		return mChannel;
	}
}
