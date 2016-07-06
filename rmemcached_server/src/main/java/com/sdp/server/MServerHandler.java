package com.sdp.server;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.manager.MessageManager;
import net.spy.memcached.MemcachedClient;
import org.jboss.netty.channel.*;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Vector;

/**
 * 
 * @author martji
 * 
 */

public class MServerHandler extends SimpleChannelUpstreamHandler {
	
	MServer mServer = null;

	private MessageManager messageManager = new MessageManager();
	
	public MServerHandler() {}

	/**
	 * 
	 * @param serverId : the id of the server instance
	 * @param serversMap : all server nodes
	 */
	public MServerHandler(int serverId, Map<Integer, ServerNode> serversMap, int protocol) {
		
		this();
		MemcachedClient mc = null;
		try {
			ServerNode serverNode = serversMap.get(serverId);
			String host = serverNode.getHost();
			int port = serverNode.getMemcached();
			mc = new MemcachedClient(new InetSocketAddress(host, port));
		} catch (Exception e) {}

		messageManager.initManager(mServer, mc);
	}

	public MServerHandler(ConcurrentHashMap<String, Vector<Integer>> replicasIdMap) {
		this();
		MemcachedClient mc = null;
		try {
			ServerNode serverNode = GlobalConfigMgr.serversMap.get(GlobalConfigMgr.id);
			String host = serverNode.getHost();
			int port = serverNode.getMemcached();
			mc = new MemcachedClient(new InetSocketAddress(host, port));
		} catch (Exception e) {}

        messageManager.initManager(mServer, mc, replicasIdMap);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		handleMessage(e);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		Channel channel = e.getChannel();
		channel.close();
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
	}
	
	private void handleMessage(MessageEvent e) {
        messageManager.handleMessage(e);
	}

	/**
	 * 
	 * @param mServer : this server is used to get the monitor client
	 */
	public void setMServer(MServer mServer) {
		this.mServer = mServer;
        messageManager.setMServer(mServer);
	}

    public void initMessageManager() {
        messageManager.startHotSpotDetection();
    }
}

