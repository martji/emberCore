package com.sdp.client;

import com.sdp.server.ServerNode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author martji
 * 
 */

public class RMemcachedClientImpl implements RMemcachedClient{

	private final static int EXPTIME = 60*60*24*10;

	ConcurrentMap<String, Vector<Integer>> keyReplicaMap = new ConcurrentHashMap<String, Vector<Integer>>();
	int clientId = 0;
	
	MemcachedClient mcClient;
	EmberClient emberClient;

	public RMemcachedClientImpl(int clientId, ServerNode serverNode, ConcurrentMap<String, Vector<Integer>> keyReplicaMap) {
		String host = serverNode.getHost();
		int rport = serverNode.getRPort();
		int wport = serverNode.getWPort();
		int memcachedPort = serverNode.getMemcached();
		
		this.clientId = clientId;
		this.keyReplicaMap = keyReplicaMap;
		init(clientId, host, rport, wport, memcachedPort);
	}

	public void init() {
		init(0, "127.0.0.1", 8080, 8090, 20000);
	}

	public void shutdown() {
		emberClient.shutdown();
		mcClient.shutdown();
	}
	
	public void init(int clientNode, String host, int rport, int wport, int memcachedPort) {
		initRConnect(clientNode, host, rport, wport);
		initSpyConnect(host, memcachedPort);
	}

	public void initRConnect(int clientNode, String host, int rport, int wport) {
		emberClient = new EmberClient(keyReplicaMap, clientNode, host, rport, wport);
	}
	
	public void initSpyConnect(String host, int memcachedPort) {
		List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
		addrs.add(new InetSocketAddress(host, memcachedPort));
		try {
			mcClient = new MemcachedClient(addrs);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * get value from memcached directly
 	 */
	public String get(final String key, boolean needRegister) {
		String value = null;
		value = (String) mcClient.get(key);
		if (needRegister) {
			register2R(key);
		}
		return value;
	}

	/**
	 * get value from ember
	 * @param key
	 * @param failedId
     * @return
     */
	public String asynGet(final String key, int failedId) {
		return emberClient.get(key, failedId);
	}
	
	public boolean set(String key, String value) {
		boolean result = false;
		if (keyReplicaMap.containsKey(key)) {
			result = asynSet2R(key, value);
		} else {
			result = set2M(key, value);
		}
		return result;
	}
	
	public boolean set2M(String key, String value) {
		OperationFuture<Boolean> res = mcClient.set(key, EXPTIME, value);
		try {
			return res.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean asynSet2R(String key, String value) {
		return rsmdSet(key, value, 0);
	}
	
	public boolean rsmdSet(String key, String value, int replicasNum) {
		return emberClient.set(key, value, replicasNum);
	}
	
	/**
	 * test example
	 */
	public void get() {
		String key = Long.toString(System.nanoTime());
		System.out.println(">>request: " + key);
		System.out.println(">>response: " + get(key));
	}
	public void set() {
		String key = "testKey";
		String value = "This is a test.";
		System.out.println(">>request: " + key + ", " + value);
		System.out.println(">>response: " + set(key, value));
	}

	public boolean delete(String key) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public void register2R(String key) {
		emberClient.register(key, clientId);
	}

	public void bindKeyReplicaMap(ConcurrentMap<String, Vector<Integer>> keyReplicaMap) {
		this.keyReplicaMap = keyReplicaMap;
	}

	public String get(String key) {
		return get(key, false);
	}
}
