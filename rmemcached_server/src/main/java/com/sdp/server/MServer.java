package com.sdp.server;

import com.sdp.client.RMClient;
import com.sdp.config.GlobalConfigMgr;
import com.sdp.example.Log;
import com.sdp.monitor.LocalMonitor;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

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
	 * @param serversMap : all the server instances info
	 */
	public void init(int id, Map<Integer, ServerNode> serversMap, int protocol) {
		ServerNode serverNode = serversMap.get(id);
		wServerHandler = new MServerHandler(id, serversMap, protocol);
		rServerHandler = new MServerHandler(id, serversMap, protocol);
		int rport = serverNode.getRPort();
		int wport = serverNode.getWPort();
		initRServer(rport, wport);
		registerMonitor();
	}

	public void init(int id, Map<Integer, ServerNode> serversMap,
			MServerHandler wServerHandler, MServerHandler rServerHandler) {
		this.wServerHandler = wServerHandler;
		this.rServerHandler = rServerHandler;
		ServerNode serverNode = serversMap.get(id);
		
		int rport = serverNode.getRPort();
		int wport = serverNode.getWPort();
		initRServer(rport, wport);
		
//		wServerHandler.replicasMgr.initThread();
//		rServerHandler.replicasMgr.initThread();
		
		registerMonitor();
	}

	public void init(MServerHandler wServerHandler, MServerHandler rServerHandler) {
		init(GlobalConfigMgr.id, GlobalConfigMgr.serversMap, wServerHandler, rServerHandler);
	}

	private void registerMonitor() {
		registerMonitor(GlobalConfigMgr.id, (String) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.MONITOR_ADDRESS),
				GlobalConfigMgr.serversMap.get(GlobalConfigMgr.id).getMemcached());
	}

	/**
	 * 
	 * @param id : the id of the server instance
	 * @param monitorAddress : the address of the monitor node
	 * @param memcachedPort : the memcachedPort 
	 */
	private void registerMonitor(int id, String monitorAddress, int memcachedPort) {
		LocalMonitor.getInstance().setPort(memcachedPort);
		Log.log.info("[monitor]: " + monitorAddress);
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

		Log.log.info("[Netty] server start.");
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
