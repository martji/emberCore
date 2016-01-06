package com.sdp.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.sdp.common.EMSGID;
import com.sdp.future.MCallback;
import com.sdp.future.MFuture;
import com.sdp.messageBody.CtsMsg.nr_read;
import com.sdp.messageBody.CtsMsg.nr_register;
import com.sdp.messageBody.CtsMsg.nr_write;
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

public class RMemcachedClientImpl implements RMemcachedClient{

	ClientBootstrap bootstrap;
	StringBuffer message = new StringBuffer();
	ConcurrentMap<String, Vector<Integer>> keyReplicaMap = new ConcurrentHashMap<String, Vector<Integer>>();
	
	static int SAMPRATE = 3;
	ExecutorService threadPool = Executors.newCachedThreadPool();
	
	int clientId = 0;
	Channel mChannel = null;
	RMemcachedClientImplHandler mClientHandler;
	MemcachedClient client;
	private static int timeout = 2500;

	public RMemcachedClientImpl(ServerNode serverNode, ConcurrentMap<String, Vector<Integer>> keyReplicaMap) {
		int serverId = serverNode.getId();
		String host = serverNode.getHost();
		int port = serverNode.getPort();
		int memcachedPort = serverNode.getMemcached();
		
		this.clientId = serverId;
		this.keyReplicaMap = keyReplicaMap;
		init(serverId, host, port, memcachedPort);
	}

	public void init() {
		init(0, "127.0.0.1", 8080, 20000);
	}

	public void shutdown() {
		mChannel.close();
		bootstrap.releaseExternalResources();
		client.shutdown();
	}
	
	public void init(int clientNode, String host, int port, int memcachedPort) {
		initRConnect(clientNode, host, port);
		initSpyConnect(host, memcachedPort);
	}

	public void initRConnect(int clientNode, String host, int port) {
		try {
			bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
					Executors.newCachedThreadPool(),
					Executors.newCachedThreadPool()));

			mClientHandler = new RMemcachedClientImplHandler(clientNode, message, keyReplicaMap);
			bootstrap.setPipelineFactory(new MClientPipelineFactory(mClientHandler));

			ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port)).sync();
			while (!future.isDone()) {}
			mChannel = future.getChannel();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void initSpyConnect(String host, int memcachedPort) {
		List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
		addrs.add(new InetSocketAddress(host, memcachedPort));
		try {
			client = new MemcachedClient(addrs);
		} catch (IOException e) {
			e.printStackTrace();
		}
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

	public String get(final String key, boolean needRegister) {
		String value = null;
		value = (String) client.get(key);
		if (needRegister) {
			register2R(key);
		}
		return value;
	}
	
	public String asynGet(final String key, int failedId) {
		CountDownLatch latch = new CountDownLatch(1);
		BaseOperation<String> op = new BaseOperation<String>(new MCallback<String>(latch));
		MFuture<String> future = new MFuture<String>(latch, op);
		String id = Long.toString(System.currentTimeMillis());
		mClientHandler.addOpMap(id + ":" + key, op);
		
		nr_read.Builder builder = nr_read.newBuilder();
		builder.setKey(key);
		NetMsg msg = NetMsg.newMessage();
		msg.setNodeRoute(failedId);
		msg.setMessageLite(builder);
		msg.setMsgID(EMSGID.nr_read);
		
		mChannel.write(msg);
		
		try {
			return future.get(timeout , TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public boolean set(String key, String value) {
		boolean result = false;
		if (keyReplicaMap.containsKey(key)) {
			result = set2R(key, value);
		} else {
			result = set2M(key, value);
		}
		return result;
	}
	
	public boolean set2M(String key, String value) {
		OperationFuture<Boolean> res = client.set(key, 3600, value);
		try {
			return res.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean aysnSet2R(String key, String value) {
		CountDownLatch latch = new CountDownLatch(1);
		BaseOperation<Boolean> op = new BaseOperation<Boolean>(new MCallback<Boolean>(latch));
		MFuture<Boolean> future = new MFuture<Boolean>(latch, op);
		String id = Long.toString(System.currentTimeMillis());
		mClientHandler.addOpMap(id + ":" + key, op);
		
		nr_write.Builder builder = nr_write.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		NetMsg msg = NetMsg.newMessage();
		msg.setNodeRoute(clientId);
		msg.setMessageLite(builder);
		msg.setMsgID(EMSGID.nr_write);
		
		mChannel.write(msg);
		
		try {
			return future.get(timeout , TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean set2R(String key, String value) {
		String result = "";
		String id = Long.toString(System.nanoTime());
		
		nr_write.Builder builder = nr_write.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		builder.setTime(System.nanoTime());
		NetMsg msg = NetMsg.newMessage();
		msg.setNodeRoute(clientId);
		msg.setMessageLite(builder);
		msg.setMsgID(EMSGID.nr_write);
		
		mClientHandler.requestList.put(id, msg);
		mClientHandler.queue.push(id);

		synchronized (id) {
			synchronized (mClientHandler.lock) {
				mClientHandler.lock.notify();
			}
			try {
				id.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		result = message.toString();
		return !result.isEmpty();
	}
	
	/**
	 * test example
	 */
	public void get() {
		String key = Long.toString(System.nanoTime());
		System.out.println(">>request: " + key);
		System.out.println(">>response: " + get(key));
	}
	public void set() {
		String key = "testKey";
		String value = "This is a test.";
		System.out.println(">>request: " + key + ", " + value);
		System.out.println(">>response: " + set(key, value));
	}

	public boolean delete(String key) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public void register2R(String key) {
//		if (need2Register()) {
		if (true) {
			nr_register.Builder builder = nr_register.newBuilder();
			builder.setKey(key);
			builder.setTime(System.nanoTime());
			NetMsg msg = NetMsg.newMessage();
			msg.setNodeRoute(clientId);
			msg.setMessageLite(builder);
			msg.setMsgID(EMSGID.nr_register);
			mChannel.write(msg);
		}
	}
	
	public boolean need2Register() {
		Random random = new Random();
		return random.nextInt(100) < SAMPRATE;
	}

	public void bindKeyReplicaMap(ConcurrentMap<String, Vector<Integer>> keyReplicaMap) {
		this.keyReplicaMap = keyReplicaMap;
	}

	public String get(String key) {
		return get(key, false);
	}
}
