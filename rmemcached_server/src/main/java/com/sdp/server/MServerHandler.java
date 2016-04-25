package com.sdp.server;

import com.sdp.config.GlobalConfigMgr;
import com.sdp.db.DBClientInterface;
import com.sdp.db.SpyMcClient;
import com.sdp.replicas.ReplicasMgr;
import org.jboss.netty.channel.*;
import org.jboss.netty.util.internal.ConcurrentHashMap;

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

	/**
	 * 
	 * @param serverId : the id of the server instance
	 * @param serversMap : all server nodes
	 */
	public MServerHandler(int serverId, Map<Integer, ServerNode> serversMap, int protocol) {
		
		this();
		replicasMgr = new ReplicasMgr(serverId, serversMap, mServer, protocol);
		DBClientInterface db = getDB();
		try {
			ServerNode serverNode = serversMap.get(serverId);
			String host = serverNode.getHost();
			int port = serverNode.getMemcached();
			if (db == null) {
				db = new SpyMcClient(host, port);
			} else {
				db.setDB(host, port);
			}
		} catch (Exception e) {}
		replicasMgr.setDBClient(db);
	}

	public MServerHandler(ConcurrentHashMap<String, Vector<Integer>> replicasIdMap) {
		this();
		replicasMgr = new ReplicasMgr(mServer, replicasIdMap);
		DBClientInterface db = getDB();
		try {
			ServerNode serverNode = GlobalConfigMgr.serversMap.get(GlobalConfigMgr.id);
			String host = serverNode.getHost();
			int port = serverNode.getMemcached();
			if (db == null) {
				db = new SpyMcClient(host, port);
			} else {
				db.setDB(host, port);
			}
		} catch (Exception e) {}
		replicasMgr.setDBClient(db);
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

	public DBClientInterface getDB() {
		try {
			Class<DBClientInterface> db = (Class<DBClientInterface>) Class.forName((String)
					GlobalConfigMgr.propertiesMap.get(GlobalConfigMgr.DB_NAME));
			return db.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}

