package com.sdp.client.others;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.sdp.client.ember.EmberDataClient;

import com.sdp.server.ServerNode;
/**
 * 
 * @author martji
 *
 */

public class MClient {

	int clientId;
	Map<Integer, EmberDataClient> clientMap;
	Vector<Integer> clientIdVector;
	/**
	 * keyReplicaMap : save the replicated key and the replica
	 */
	ConcurrentHashMap<String, Vector<Integer>> keyReplicaMap;
	
	/**
	 * 
	 * @param clientId
	 * @param serversMap
	 */
	public MClient(int clientId, Map<Integer, ServerNode> serversMap) {
		this(clientId);
		init(serversMap);
	}
	
	public MClient(int clientId) {
		this.clientId = clientId;
		clientMap = new HashMap<Integer, EmberDataClient>();
		keyReplicaMap = new ConcurrentHashMap<String, Vector<Integer>>();
		clientIdVector = new Vector<Integer>();
	}
	
	public void init(Map<Integer, ServerNode> serversMap) {
		Collection<ServerNode> serverList = serversMap.values();
		for (ServerNode serverNode : serverList) {
			int serverId = serverNode.getId();
			EmberDataClient rmClient = new EmberDataClient(serverNode, keyReplicaMap);
			clientMap.put(serverId, rmClient);
			clientIdVector.add(serverId);
		}
		Collections.sort(clientIdVector);
	}
	
	public void shutdown() {
		Collection<EmberDataClient> clientList = clientMap.values();
		for (EmberDataClient mClient : clientList) {
			mClient.shutdown();
		}
	}
	
	/**
	 * 
	 * @param key
	 * @return get the hash value of the key and map it to the serverId
	 */
	public int gethashMem(String key) {
		int clientsNum = clientIdVector.size();
		if (clientsNum == 1) {
			return 0;
		}
		int leaderIndex = key.hashCode() % clientsNum;
		leaderIndex = Math.abs(leaderIndex);
		return clientIdVector.get(leaderIndex);
	}
	
	public int getOneReplica(String key) {
		Vector<Integer> vector = keyReplicaMap.get(key);
		int index = new Random().nextInt(vector.size());
		return vector.get(index);
	}
	
	public String get(String key) {
		String value = null;
		EmberDataClient rmClient;
		int masterId = gethashMem(key);
		
		if (keyReplicaMap.containsKey(key)) {
			int replicasId = getOneReplica(key);
			rmClient = clientMap.get(replicasId);
			value = rmClient.get(key, masterId == replicasId);
			if (value == null | value.length() == 0) {
				rmClient = clientMap.get(masterId);
				value = rmClient.asyncGetFromEmber(key, replicasId);
			}
		} else {
			rmClient = clientMap.get(masterId);
			value = rmClient.get(key, true);
		}
		return value;
	}
	
	public boolean set(String key, String value) {
		boolean result = false;
		int masterId = gethashMem(key);
		EmberDataClient rmClient = clientMap.get(masterId);
		result = rmClient.set(key, value);
		return result;
	}
}
