package com.sdp.server;

import com.sdp.client.RMClient;
import com.sdp.config.GlobalConfigMgr;
import com.sdp.log.Log;
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

	public static final int SLEEP_TIME = 30 * 1000;

	public MServer() {}

	/**
	 * Init the server, and register to the monitor.
	 * @param id
	 * @param serversMap
	 * @param wServerHandler
	 * @param rServerHandler
     */
	public void init(int id, Map<Integer, ServerNode> serversMap,
			MServerHandler wServerHandler, MServerHandler rServerHandler) {
		this.wServerHandler = wServerHandler;
		this.rServerHandler = rServerHandler;
		ServerNode serverNode = serversMap.get(id);
		
		int rPort = serverNode.getRPort();
		int wPort = serverNode.getWPort();
		initRServer(rPort, wPort);
		
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
	 * @param serverPort : the serverPort
	 */
	private void registerMonitor(int id, String monitorAddress, int serverPort) {
		LocalMonitor.getInstance().setPort(serverPort);
		Log.log.info("[monitor]: " + monitorAddress);
		String[] arr = monitorAddress.split(":");
		
		final String host = arr[0];
		final int port = Integer.parseInt(arr[1]);
		monitorClient = new RMClient(id, host, port);
		while (monitorClient.getmChannel() == null) {
			try {
				Thread.sleep(SLEEP_TIME);
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
						Thread.sleep(SLEEP_TIME * 2);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (!monitorClient.getmChannel().isConnected()) {
						while (!monitorClient.getmChannel().isConnected()) {
							monitorClient.connect(host, port);
							try {
								Thread.sleep(SLEEP_TIME);
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

	public void initRServer(int rPort, int wPort) {
		ServerBootstrap wBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		wBootstrap.setPipelineFactory(new MServerPipelineFactory(wServerHandler));
		wBootstrap.setOption("child.tcpNoDelay", true);
		wBootstrap.setOption("child.keepAlive", true);
		wBootstrap.setOption("reuseAddress", true);
		wBootstrap.bind(new InetSocketAddress(wPort));
		
		ServerBootstrap rBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		rBootstrap.setPipelineFactory(new MServerPipelineFactory(rServerHandler));
		rBootstrap.setOption("child.tcpNoDelay", true);
		rBootstrap.setOption("child.keepAlive", true);
		rBootstrap.setOption("reuseAddress", true);
		rBootstrap.bind(new InetSocketAddress(rPort));

		Log.log.info("[Netty] server start.");
	}

	public String getAReplica() {
		if (monitorClient != null) {
			return monitorClient.asynGetAReplica();
		}
		return null;
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