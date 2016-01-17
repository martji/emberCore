package com.sdp.server;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.replicas.ReplicasMgr;
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
	
	public ReplicasMgr replicasMgr;
	MServer mServer = null;
	
	public MServerHandler() {}

	public MServerHandler(ConcurrentHashMap<String, Vector<Integer>> replicasIdMap) {
        int serverId = GlobalConfigMgr.id;
        Map<Integer, ServerNode> serversMap = GlobalConfigMgr.serversMap;
        int protocol = (Integer) GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.REPLICA_PROTOCOL);
        new MServerHandler(serverId, serversMap, protocol, replicasIdMap);
	}
	
	/**
	 * 
	 * @param serverId : the id of the server instance
	 * @param serversMap : all server nodes
	 */
	public MServerHandler(int serverId, Map<Integer, ServerNode> serversMap, int protocol) {
		
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

	public MServerHandler(int serverId,
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

