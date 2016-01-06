package com.sdp.server;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.sdp.client.RMClient;
import com.sdp.monitor.LocalMonitor;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;

/**
 * 
 * @author martji
 * 
 */

public class MServer {
	MServerHandler wServerHandler;
	MServerHandler rServerHandler;
	RMClient monitorClient;

	public MServer() {}
	
	/**
	 * 
	 * @param id : the id of the server instance
	 * @param monitorAddress : the address of the monitor node
	 * @param serversMap : all the server instances info
	 */
	public void init(int id, String monitorAddress, Map<Integer, ServerNode> serversMap, int protocol) {
		ServerNode serverNode = serversMap.get(id);
		String server = serverNode.getServer();
		wServerHandler = new MServerHandler(server, id, serversMap, protocol);
		rServerHandler = new MServerHandler(server, id, serversMap, protocol);
		int rport = serverNode.getRPort();
		int wport = serverNode.getWPort();
		initRServer(rport, wport);
		registerMonitor(id, monitorAddress, serversMap.get(id).getMemcached());
	}

	public void init(int id, String monitorAddress, Map<Integer, ServerNode> serversMap, 
			MServerHandler wServerHandler, MServerHandler rServerHandler) {
		this.wServerHandler = wServerHandler;
		this.rServerHandler = rServerHandler;
		ServerNode serverNode = serversMap.get(id);
		
		int rport = serverNode.getRPort();
		int wport = serverNode.getWPort();
		initRServer(rport, wport);
		
		wServerHandler.replicasMgr.initThread();
		rServerHandler.replicasMgr.initThread();
		
		registerMonitor(id, monitorAddress, serversMap.get(id).getMemcached());
	}
	
	/**
	 * 
	 * @param id : the id of the server instance
	 * @param monitorAddress : the address of the monitor node
	 * @param memcachedPort : the memcachedPort 
	 */
	private void registerMonitor(int id, String monitorAddress, int memcachedPort) {
		LocalMonitor.getInstance().setPort(memcachedPort);
		String[] arr = monitorAddress.split(":");
		
		final String host = arr[0];
		final int port = Integer.parseInt(arr[1]);
		monitorClient = new RMClient(id, host, port);
		while (monitorClient.getmChannel() == null) {
			try {
				Thread.sleep(30*1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			monitorClient.connect(host, port);
		}
		LocalMonitor.getInstance().setMonitorChannel(monitorClient.getmChannel());
		
		new Thread(new Runnable() {
			public void run() {
				while (true) {
					try {
						Thread.sleep(60*1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (!monitorClient.getmChannel().isConnected()) {
						while (!monitorClient.getmChannel().isConnected()) {
							monitorClient.connect(host, port);
							try {
								Thread.sleep(30*1000);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						LocalMonitor.getInstance().setMonitorChannel(monitorClient.getmChannel());
					}
				}
			}
		}).start();
	}

	public void initRServer(int rport, int wport) {
		ServerBootstrap wbootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		wbootstrap.setPipelineFactory(new MServerPipelineFactory(wServerHandler));
		wbootstrap.setOption("child.tcpNoDelay", true);
		wbootstrap.setOption("child.keepAlive", true);
		wbootstrap.setOption("reuseAddress", true);
		wbootstrap.bind(new InetSocketAddress(wport));
		
		ServerBootstrap rbootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		rbootstrap.setPipelineFactory(new MServerPipelineFactory(rServerHandler));
		rbootstrap.setOption("child.tcpNoDelay", true);
		rbootstrap.setOption("child.keepAlive", true);
		rbootstrap.setOption("reuseAddress", true);
		rbootstrap.bind(new InetSocketAddress(rport));
		
		System.out.println("[Netty] server start.");
	}

	public String getAReplica() {
		return monitorClient.asynGetAReplica();
	}

	private class MServerPipelineFactory implements ChannelPipelineFactory {
		MServerHandler mServerHandler;
		
		public MServerPipelineFactory(MServerHandler mServerHandler) {
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
	
}
