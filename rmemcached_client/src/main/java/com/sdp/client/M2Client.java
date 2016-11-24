package com.sdp.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import com.sdp.server.ServerNode;

public class M2Client {
	
	Map<Integer, MemcachedClient> clientMap;
	Vector<Integer> clientIdVector;
	private static int exptime = 60*60*24*10;
	int mode = 0;
	int recordCount = 1;

	public M2Client(int mode, int recordCount, Map<Integer, ServerNode> serversMap) {
		clientMap = new HashMap<Integer, MemcachedClient>();
		clientIdVector = new Vector<Integer>();
		this.mode = mode;
		this.recordCount = recordCount;
		
		Collection<ServerNode> serverList = serversMap.values();
		for (ServerNode serverNode : serverList) {
			int serverId = serverNode.getId();
			String host = serverNode.getHost();
			int memcachedPort = serverNode.getMemcached();
			List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
			addrs.add(new InetSocketAddress(host, memcachedPort));
			try {
				MemcachedClient mClient = new MemcachedClient(addrs);
				clientMap.put(serverId, mClient);
				clientIdVector.add(serverId);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(clientIdVector);
	}
	
	public String get(String key) {
		String value = null;
		MemcachedClient mClient;
		int masterId;
		if (mode == 0) {
			masterId = gethashMem(key);
		} else if (mode == 2){
			masterId = getOneClient();
		} else {
			masterId = getSliceMem(key);
		}
		mClient = clientMap.get(masterId);
		value = (String) mClient.get(key);
		return value;
	}
	
	public int getSliceMem(String key) {
		int index = key.lastIndexOf("user") + 4;
		int keyNum = Integer.decode(key.substring(index));
		
		int clientsNum = clientIdVector.size();
		if (clientsNum == 1) {
			return 0;
		}
		int gap = this.recordCount / clientsNum;
		int leaderIndex = keyNum / gap;
		leaderIndex = Math.abs(leaderIndex);
		return clientIdVector.get(leaderIndex);
	}
	
	public int gethashMem(String key) {
		int clientsNum = clientIdVector.size();
		if (clientsNum == 1) {
			return 0;
		}
		int leaderIndex = key.hashCode() % clientsNum;
		leaderIndex = Math.abs(leaderIndex);
		return clientIdVector.get(leaderIndex);
	}
	
	public int getOneClient() {
		int index = new Random().nextInt(clientIdVector.size());
		return clientIdVector.get(index);
	}
	
	public boolean set(String key, String value) {
		int masterId;
		if (mode == 1) {
			masterId = getSliceMem(key);
		} else {
			masterId = gethashMem(key);
		}
		MemcachedClient mClient = clientMap.get(masterId);
		OperationFuture<Boolean> res = mClient.set(key, exptime, value);
		try {
			return res.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public void shutdown() {
		Collection<MemcachedClient> clients = clientMap.values();
		for (MemcachedClient client : clients) {
			client.shutdown();
		}
	}
}
