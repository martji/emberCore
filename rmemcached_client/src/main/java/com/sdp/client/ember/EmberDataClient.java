package com.sdp.client.ember;

import com.sdp.client.DataClientFactory;
import com.sdp.client.interfaces.DataClient;
import com.sdp.server.ServerNode;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author martji
 * 
 */

public class EmberDataClient implements DataClient {

    private DataClient dataClient;
	private EmberClient emberClient;

	ConcurrentHashMap<String, Vector<Integer>> replicaTable;

	public EmberDataClient(ServerNode serverNode, ConcurrentHashMap<String, Vector<Integer>> replicaTable) {
		this.replicaTable = replicaTable;
		init(serverNode);
	}
	
	public void init(ServerNode serverNode) {
		dataClient = DataClientFactory.createInstance(DataClientFactory.MC_MODE, serverNode, replicaTable);
		emberClient = new EmberClient(serverNode, replicaTable);
	}

	public void init() {}

    public void shutdown() {
        emberClient.shutdown();
        dataClient.shutdown();
    }

    public String get(String key) {
        return get(key, false);
    }

    public boolean set(String key, String value) {
        boolean result;
        if (replicaTable.containsKey(key)) {
            result = set2Ember(key, value);
        } else {
            result = set2DataServer(key, value);
        }
        return result;
    }

    public boolean delete(String key) {
        return false;
    }

	/**
	 * get value from data server directly, only the master server record the register
 	 */
	public String get(String key, boolean needRegister) {
		String value = dataClient.get(key);
		if (needRegister) {
            emberClient.register(key);
		}
		return value;
	}

	/**
	 * get value from ember
	 * @param key
	 * @param failedId
     * @return
     */
	public String asyncGetFromEmber(String key, int failedId) {
		return emberClient.get(key, failedId);
	}
	
	public boolean set2DataServer(String key, String value) {
		return dataClient.set(key, value);
	}

    public boolean set2Ember(String key, String value) {
        return asyncSet2Ember(key, value, 0);
    }
	
	public boolean asyncSet2Ember(String key, String value, int replicaNum) {
        return emberClient.set(key, value, replicaNum);
	}
}
