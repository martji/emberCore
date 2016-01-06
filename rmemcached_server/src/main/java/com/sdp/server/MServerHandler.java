package com.sdp.server;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Vector;

import net.spy.memcached.MemcachedClient;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.sdp.replicas.ReplicasMgr;

/**
 * 
 * @author martji
 * 
 */

public class MServerHandler extends SimpleChannelUpstreamHandler {
	
	public ReplicasMgr replicasMgr;
	MServer mServer = null;
	
	public MServerHandler() {}
	
	/**
	 * 
	 * @param server : the address of memcached server
	 * @param serverId : the id of the server instance
	 * @param serversMap : all server nodes
	 */
	public MServerHandler(String server, int serverId, Map<Integer, ServerNode> serversMap, int protocol) {
		
		this();
		replicasMgr = new ReplicasMgr(serverId, serversMap, mServer, protocol);
		
		MemcachedClient mc = null;
		try {
			ServerNode serverNode = serversMap.get(serverId);
			String host = serverNode.getHost();
			int memcachedPort = serverNode.getMemcached();
			mc = new MemcachedClient(new InetSocketAddress(host, memcachedPort));
		} catch (Exception e) {}
		replicasMgr.setMemcachedClient(mc);
	}

	public MServerHandler(String server, int serverId,
			Map<Integer, ServerNode> serversMap, int protocol,
			ConcurrentHashMap<String, Vector<Integer>> replicasIdMap) {
		this();
		replicasMgr = new ReplicasMgr(serverId, serversMap, mServer, protocol, replicasIdMap);
		
		MemcachedClient mc = null;
		try {
			ServerNode serverNode = serversMap.get(serverId);
			String host = serverNode.getHost();
			int memcachedPort = serverNode.getMemcached();
			mc = new MemcachedClient(new InetSocketAddress(host, memcachedPort));
		} catch (Exception e) {}
		replicasMgr.setMemcachedClient(mc);
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
		replicasMgr.handle(e);
	}

	/**
	 * 
	 * @param mServer : this server is used to get the monitor client
	 */
	public void setMServer(MServer mServer) {
		this.mServer = mServer;
		replicasMgr.setMServer(mServer);
	}
}

