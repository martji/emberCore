package com.sdp.server;

import net.spy.memcached.MemcachedClient;

public interface MCPool {
	
	public void initialize();
	public MemcachedClient getMClient(String key);
	public MemcachedClient getMClient();
	public void recoverMClient(MemcachedClient mc);
	public MemcachedClient createMClient();
	public void shutdown();
}
