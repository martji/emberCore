package com.sdp.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.spy.memcached.MemcachedClient;

public class MCPoolImpl implements MCPool {

	int minClientNum;
	List<InetSocketAddress> mcAddrs;
	
	ConcurrentLinkedQueue<MemcachedClient> availablePool = new ConcurrentLinkedQueue<MemcachedClient>();
	ConcurrentLinkedQueue<MemcachedClient> busyPool = new ConcurrentLinkedQueue<MemcachedClient>();
	
	public MCPoolImpl(int minClientNum, List<InetSocketAddress> mcAddrs) {
		this.minClientNum = minClientNum;
		this.mcAddrs = mcAddrs;
		
		initialize();
	}
	
	public void initialize() {
		for (int i = 0; i < minClientNum; i++) {
			MemcachedClient mc = createMClient();
			if (mc != null) {
				availablePool.add(mc);
			}
		}
	}

	public MemcachedClient getMClient(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	public MemcachedClient getMClient() {
		// TODO Auto-generated method stub
		MemcachedClient mc;
		if (availablePool.isEmpty()) {
			mc = createMClient();
		} else {
			mc = availablePool.poll();
			busyPool.add(mc);
		}
		return mc;
	}

	public MemcachedClient createMClient() {
		try {
			MemcachedClient mc = new MemcachedClient(mcAddrs);
			busyPool.add(mc);
			return mc;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	public void recoverMClient(MemcachedClient mc) {
		// TODO Auto-generated method stub
		availablePool.add(mc);
		busyPool.remove(mc);
	}

}
